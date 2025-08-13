package de.skyrising.guardian.gen.mappings

import de.skyrising.guardian.gen.*
import org.objectweb.asm.*
import org.objectweb.asm.commons.ClassRemapper
import org.objectweb.asm.commons.Remapper
import org.objectweb.asm.tree.*
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.regex.Pattern
import javax.lang.model.SourceVersion

class AsmRemapper(private val tree: MappingTree, private val superClasses: Map<String, Set<String>>, private val namespace: Int = tree.namespaces.size - 1) : Remapper() {
    override fun map(internalName: String?): String? {
        if (internalName == null) return null
        return tree.mapType(internalName, namespace)
    }

    override fun mapRecordComponentName(owner: String?, name: String?, descriptor: String?): String? {
        return mapFieldName(owner, name, descriptor)
    }

    override fun mapFieldName(owner: String?, name: String?, descriptor: String?): String? {
        if (owner == null || name == null || descriptor == null) return null
        return mapFieldName0(tree.classes[owner], owner, MemberDescriptor(name, descriptor)) ?: name
    }

    private fun mapFieldName0(owner: ClassMapping?, ownerName: String, member: MemberDescriptor): String? =
        mapMember(owner, ownerName, member, { o, f -> o.fields[f] }, ::mapFieldName0)

    override fun mapMethodName(owner: String?, name: String?, descriptor: String?): String? {
        if (owner == null || name == null || descriptor == null) return null
        return mapMethodName0(tree.classes[owner], owner, MemberDescriptor(name, descriptor)) ?: name
    }

    private fun mapMethodName0(owner: ClassMapping?, ownerName: String, member: MemberDescriptor): String? =
        mapMember(owner, ownerName, member, { o, m -> o.methods[m] }, ::mapMethodName0)

    // fallen: add the ownerName parameter.
    // for those whose owner class mapping do not exist, their superclass class mapping might still exist. give them a chance
    private inline fun mapMember(owner: ClassMapping?, ownerName: String, member: MemberDescriptor,
                                 getMember: (ClassMapping, MemberDescriptor) -> MemberMapping?,
                                 recurse: (ClassMapping?, String, MemberDescriptor) -> String?
    ): String? {
        if (owner != null) {
            val mapped = getMember(owner, member)
            if (mapped != null) return mapped.getName(namespace)
        }
        val supers = superClasses[ownerName] ?: return null
        for (superClass in supers) {
            val superMapped = recurse(tree.classes[superClass], superClass, member)
            if (superMapped != null) return superMapped
        }
        return null
    }

    override fun mapValue(value: Any?): Any? {
        if (value is Handle) {
            return Handle(
                value.tag,
                mapType(value.owner),
                if (value.tag <= Opcodes.H_PUTSTATIC) {
                    mapFieldName(value.owner, value.name, value.desc)
                } else {
                    mapMethodName(value.owner, value.name, value.desc)
                },
                if (value.tag <= Opcodes.H_PUTSTATIC) {
                    mapDesc(value.desc)
                } else {
                    mapMethodDesc(value.desc)
                },
                value.isInterface
            )
        }
        return super.mapValue(value)
    }
}

fun getMappedJarOutput(provider: String, input: Path): Path = JARS_MAPPED_DIR.resolve(provider).resolve(JARS_DIR.relativize(input))

fun mapJar(version: String, input: Path, mappings: MappingTree, provider: String, namespace: Int = mappings.namespaces.size - 1): CompletableFuture<Path> {
    val output = getMappedJarOutput(provider, input)
    return mapJar(version, input, output, mappings, namespace).thenApply { output }
}

fun mapJar(version: String, input: Path, output: Path, mappings: MappingTree, namespace: Int = mappings.namespaces.size - 1) = supplyAsync(TaskType.REMAP) {
    getJarFileSystem(input).use { inFs ->
        fun createSuperClassesMapping(classNodes: Map<String, ClassNode>): Map<String, MutableSet<String>> {
            val superClasses = mutableMapOf<String, MutableSet<String>>()
            classNodes.entries.forEach { (className, classNode) ->
                val supers = linkedSetOf<String>()
                val superName = classNode.superName
                if (superName != null) supers.add(superName)
                supers.addAll(classNode.interfaces)
                superClasses[className] = supers
            }
            return superClasses
        }
        fun remapOnce(classNodes: Map<String, ClassNode>, subMappings: MappingTree, idx: Int, renameLocalVar: Boolean): Map<String, ClassNode> {
            val superClasses = createSuperClassesMapping(classNodes)
            val remappedNodes = linkedMapOf<String, ClassNode>()
            Timer(version, "remapJar${idx}").use {
                val classNames = superClasses.keys
                for (supers in superClasses.values) supers.retainAll(classNames)
                val remapper = AsmRemapper(subMappings, superClasses, namespace)
                for ((className, classNode) in classNodes) {
                    val remappedName = remapper.map(className) ?: className
                    val remappedNode = ClassNode()
                    classNode.accept(ClassRemapper(remappedNode, remapper))
                    fixBridgeMethods(remappedNode)
                    remappedNodes[remappedName] = remappedNode
                }

                if (renameLocalVar) {
                    val revertedSubMappings = subMappings.invert(namespace)
                    val remappedSuperClasses = createSuperClassesMapping(remappedNodes)
                    for ((remappedName, remappedNode) in remappedNodes) {
                        renameLocalVariables(remappedName, remappedNode, revertedSubMappings, remappedSuperClasses)
                    }
                }

            }
            return remappedNodes
        }

        createJarFileSystem(output).use { outFs ->
            val inRoot = inFs.getPath("/")
            val outRoot = outFs.getPath("/")

            val readNodes = linkedMapOf<String, ClassNode>()
            Timer(version, "remapJarRead").use {
                Files.walk(inRoot).forEach {
                    if (it.parent != null) {
                        if (it.parent != null && !Files.isDirectory(it)) {
                            val inRel = inRoot.relativize(it).toString()
                            if (inRel.endsWith(".class")) {
                                val className = inRel.substring(0, inRel.length - 6)
                                val classReader = ClassReader(Files.readAllBytes(it))
                                val classNode = ClassNode()
                                classReader.accept(classNode, 0)
                                readNodes[className] = classNode
                            }
                        }
                    }
                }
            }
            var classNodes: Map<String, ClassNode> = readNodes

            val mappingTreeSequence = mutableListOf<MappingTree>()
            if (mappings is CombinedYarnMappingTree) {
                mappingTreeSequence.add(mappings.intermediary)
                mappingTreeSequence.add(mappings.yarn)
            } else {
                mappingTreeSequence.add(mappings)
            }
            mappingTreeSequence.forEachIndexed { i, sm ->
                classNodes = remapOnce(classNodes, sm, i, i == mappingTreeSequence.size - 1)
            }

            for ((className, classNode) in classNodes) {
                if (classNode.sourceFile == null) {
                    var end = className.indexOf('$')
                    if (end < 0) end = className.length
                    val start = className.lastIndexOf('/', end) + 1
                    val name = className.substring(start, end)
                    classNode.sourceFile = "$name.java"
                }
                val classWriter = ClassWriter(0)
                classNode.accept(classWriter)
                val outPath = outRoot.resolve("$className.class")
                Files.createDirectories(outPath.parent)
                Files.write(outPath, classWriter.toByteArray())
            }
        }
    }
}

class MethodVariableRenamer(private val className: String, private val methodNode: MethodNode, private val superclasses: Map<String, MutableSet<String>>, private val cmp: ClassMethodMappingProvider) {
    private val defaultLvNamePattern = Pattern.compile("^lvt\\d+$")

    fun process() {
        processParams()
        processLocalVariables()
    }

    private fun getMethodParameterMappingName(index: Int): String? {
        val cannotSearchSuper = methodNode.name == "<init>" || (methodNode.access and Opcodes.ACC_PRIVATE) != 0

        fun doGet(clazz: String): String? {
            val methodMapping = cmp.get(clazz)?.get(methodNode)
            methodMapping?.parameters?.get(index)?.name ?.let { return it }
            if (cannotSearchSuper || clazz == "java/lang/Object") return null
            val supers = superclasses[clazz]?.toList() ?: listOf()
            supers.forEach { superClazz ->
                // TODO: skip if enters a super private method
                doGet(superClazz)?.let { return it }
            }
            return null
        }
        return doGet(className)
    }

    private fun processParams() {
        val renamer = LocalVariableRenamer(superclasses)
        val params = Type.getArgumentTypes(methodNode.desc)
        if (methodNode.parameters == null) {
            methodNode.parameters = List(params.size) { ParameterNode(null, 0) }
        }
        val isStatic = (methodNode.access and Opcodes.ACC_STATIC) != 0
        val toRenameIdx = mutableListOf<Int>()
        var index = if (isStatic) 0 else 1
        for ((i, param) in methodNode.parameters.withIndex()) {
            if (i >= params.size) {
                break  // just in case
            }
            val paramName = getMethodParameterMappingName(index)
            if (!paramName.isNullOrEmpty()) {
                param.name = paramName
                renamer.markExisting(paramName)
            } else if (!param.name.isNullOrEmpty()) {
                renamer.markExisting(param.name)
            } else {
                toRenameIdx.add(i)
            }
            index += params[i].size
        }
        for (i in toRenameIdx) {
            val param = methodNode.parameters[i]
            param.name = renamer.generateNameFor(params[i].descriptor, param.name)
        }
    }

    private fun processLocalVariables() {
        if (methodNode.localVariables == null) {
            return
        }

        val isStatic = (methodNode.access and Opcodes.ACC_STATIC) != 0
        val renamer = LocalVariableRenamer(superclasses)
        val toRename = mutableListOf<LocalVariableNode>()
        for (lv in methodNode.localVariables) {
            if (!isStatic && lv.index == 0) {
                continue  // this
            }
            val paramName = getMethodParameterMappingName(lv.index)
            var needsRename = false
            if (!paramName.isNullOrEmpty()) {
                lv.name = paramName
            } else if (defaultLvNamePattern.matcher(lv.name).matches()) {
                toRename.add(lv)
                needsRename = true
            }

            if (!needsRename) {
                renamer.markExisting(lv.name)
            }
        }

        for (lv in toRename) {
            lv.name = renamer.generateNameFor(lv.desc, lv.name)
        }
    }
}

class LocalVariableRenamer(private val superclasses: Map<String, MutableSet<String>>) {
    private val names = mutableSetOf<String>()
    private val nameCounts = mutableMapOf<String, Int>()

    data class VarName(val varName: String, val incrementLetter: Boolean)

    private fun generateVarName(desc: String): VarName? {
        val type = desc.substringAfterLast('[')
        var incrementLetter = true
        return when (type[0]) {
            'B' -> "b"
            'C' -> "c"
            'D' -> "d"
            'F' -> "f"
            'I' -> "i"
            'J' -> "l"
            'S' -> "s"
            'Z' -> {
                incrementLetter = false
                "bl"
            }

            'L' -> run branch@{
                fun tryMakeNameForClassName0(clazz: String): Pair<String?, String> {
                    var className = clazz
                    className = className.substringAfterLast('/')
                    className = className.substringAfterLast('$')

                    if (className.isEmpty()) {
                        return Pair(null, "bad className, desc=$desc, className=$className")
                    }

                    val varName = className[0].lowercaseChar() + className.substring(1)
                    return if (!isValidJavaIdentifier(varName)) {
                        val nameWithoutLeadingDigit = className.dropWhile { it.isDigit() }
                        if (nameWithoutLeadingDigit.isNotEmpty() && nameWithoutLeadingDigit != className) {
                            // Anonymous classes in member functions, e.g. "net/minecraft/network/chat/Style$1Collector" in mc1.21.4 mojmap
                            tryMakeNameForClassName0(nameWithoutLeadingDigit)
                        } else {
                            Pair(null, "bad varName, desc=$desc, className=$className, varName=$varName")
                        }
                    } else if (varName == className) {
                        Pair(null, "unchanged")
                    } else {
                        Pair(varName, "")
                    }
                }

                val superErrors = mutableListOf<String>()
                fun tryMakeNameForClassName(clazz: String): Pair<String?, String> {
                    val (varName, err) = tryMakeNameForClassName0(clazz)
                    if (varName == null) {
                        val supers = superclasses[clazz]?.toList() ?: listOf()
                        supers.forEach { superClazz ->
                            if (superClazz == "java/lang/Object") return@forEach
                            val (superVarName, superErr) = tryMakeNameForClassName(superClazz)
                            if (superVarName != null) {
                                return Pair(superVarName, "")
                            } else {
                                superErrors.add("super $superClazz $superErr")
                            }
                        }
                    }
                    return Pair(varName, err)
                }

                incrementLetter = false
                val currentClassName = type.substring(minOf(1, type.length)).removeSuffix(";")  // remove the 'L' prefix and the ';' suffix
                val (varName, err) = tryMakeNameForClassName(currentClassName)
                if (varName != null) return@branch varName

                if (err != "unchanged") {
                    output("lv-rename", "rename class var failed: $err, super: $superErrors")
                }
                null
            }
            else -> {
                output("lv-rename", "bad type, type=$type")
                null
            }
        }?.let {
            VarName(it, incrementLetter)
        }
    }

    fun markExisting(name: String) {
        names.add(name)
        nameCounts[name] = 1
    }

    fun generateNameFor(desc: String, fallback: String?): String? {
        val varNameRet = generateVarName(desc) ?: return fallback
        var varName = varNameRet.varName
        val pluralVarName = varName + 's'
        val incrementLetter = varNameRet.incrementLetter && varName.length == 1

        val plural = desc[0] == '[' && !isJavaKeyword(pluralVarName)
        if (plural) {
            varName = pluralVarName
        }

        if (incrementLetter) {
            var index = varName[0] - 'a'
            while (names.contains(varName) || isJavaKeyword(varName)) {
                varName = getIndexName(++index, plural)
            }
        } else {
            var baseVarName = varName
            if (isJavaKeyword(baseVarName)) {
                baseVarName += '_'
            }

            var count: Int = nameCounts.compute(baseVarName) { _, v -> if (v == null) 1 else v + 1 }!!
            while (true) {
                varName = if (count == 1) {
                    baseVarName
                } else {
                    baseVarName + count
                }

                if (!names.contains(varName)) {
                    break
                }
                count++
            }

            nameCounts[baseVarName] = count
        }

        names.add(varName)
        return varName
    }

    private fun getIndexName(index: Int, plural: Boolean): String {
        val sb = StringBuilder(2)
        var value = index
        while (value > 0) {
            sb.append('a' + value % 26)
            value /= 26
        }
        var name = sb.toString()
        if (plural) {
            name += 's'
        }
        return name
    }

    private fun isValidJavaIdentifier(s: String): Boolean {
        return s.isNotEmpty() && SourceVersion.isIdentifier(s) && s.codePoints().noneMatch { Character.isIdentifierIgnorable(it) }
    }

    private fun isJavaKeyword(s: String): Boolean {
        return SourceVersion.isKeyword(s)
    }
}

fun interface MethodMappingProvider {
    fun get(methodNode: MethodNode): MethodMapping?
}

fun interface ClassMethodMappingProvider {
    fun get(className: String): MethodMappingProvider?
}

fun renameLocalVariables(remappedName: String, remappedNode: ClassNode, mapping: MappingTree, superclasses: Map<String, MutableSet<String>>) {
    val cmp = ClassMethodMappingProvider { className ->
        val cm = mapping.classes[className]?:return@ClassMethodMappingProvider null
        MethodMappingProvider { methodNode -> cm.methods[MemberDescriptor(methodNode.name, methodNode.desc)]}
    }
    for (methodNode in remappedNode.methods) {
        MethodVariableRenamer(remappedName, methodNode, superclasses, cmp).process()
    }
}

private fun fixBridgeMethods(node: ClassNode) {
    for (m in node.methods) {
        val synthetic = (m.access and Opcodes.ACC_SYNTHETIC) != 0
        val bridge = (m.access and Opcodes.ACC_BRIDGE) != 0
        if (!synthetic || bridge) continue
        val args = Type.getArgumentTypes(m.desc)
        var callsSpecialized = false
        var callsOthers = false
        for (insn in m.instructions) {
            if (insn !is MethodInsnNode) continue
            if (isProbableBridgeCall(node, m, args, insn)) {
                callsSpecialized = true
            } else {
                callsOthers = true
                break
            }
        }
        if (callsSpecialized && !callsOthers) {
            m.access = m.access or Opcodes.ACC_BRIDGE
        }
    }
}

private fun isProbableBridgeCall(node: ClassNode, bridge: MethodNode, bridgeArgs: Array<Type>, insn: MethodInsnNode): Boolean {
    if (insn.itf || insn.name != bridge.name || !isInHierarchy(node, insn.owner)) return false
    val targetArgs = Type.getArgumentTypes(insn.desc)!!
    // TODO: check argument castability, possibly not decidable
    return targetArgs.size == bridgeArgs.size
}
private fun isInHierarchy(node: ClassNode, name: String) = name == node.name || name == node.superName || name in node.interfaces
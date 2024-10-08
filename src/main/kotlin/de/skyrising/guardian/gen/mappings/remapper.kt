package de.skyrising.guardian.gen.mappings

import de.skyrising.guardian.gen.*
import org.objectweb.asm.*
import org.objectweb.asm.commons.ClassRemapper
import org.objectweb.asm.commons.Remapper
import org.objectweb.asm.tree.ClassNode
import org.objectweb.asm.tree.LocalVariableNode
import org.objectweb.asm.tree.MethodInsnNode
import org.objectweb.asm.tree.MethodNode
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
        return mapFieldName0(tree.classes[owner], MemberDescriptor(name, descriptor)) ?: name
    }

    private fun mapFieldName0(owner: ClassMapping?, member: MemberDescriptor): String? =
        mapMember(owner, member, { o, f -> o.fields[f] }, ::mapFieldName0)

    override fun mapMethodName(owner: String?, name: String?, descriptor: String?): String? {
        if (owner == null || name == null || descriptor == null) return null
        return mapMethodName0(tree.classes[owner], MemberDescriptor(name, descriptor)) ?: name
    }

    private fun mapMethodName0(owner: ClassMapping?, member: MemberDescriptor): String? =
        mapMember(owner, member, { o, m -> o.methods[m] }, ::mapMethodName0)

    private inline fun mapMember(owner: ClassMapping?, member: MemberDescriptor,
                                 getMember: (ClassMapping, MemberDescriptor) -> MemberMapping?,
                                 recurse: (ClassMapping?, MemberDescriptor) -> String?
    ): String? {
        if (owner == null) return null
        val mapped = getMember(owner, member)
        if (mapped != null) return mapped.getName(namespace)
        val supers = superClasses[owner.defaultName] ?: return null
        for (superClass in supers) {
            val superMapped = recurse(tree.classes[superClass], member)
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
        createJarFileSystem(output).use { outFs ->
            val inRoot = inFs.getPath("/")
            val outRoot = outFs.getPath("/")
            val classNodes = linkedMapOf<String, ClassNode>()
            val superClasses = mutableMapOf<String, MutableSet<String>>()
            Timer(version, "remapJarIndex").use {
                Files.walk(inRoot).forEach {
                    if (it.parent != null) {
                        if (it.parent != null && !Files.isDirectory(it)) {
                            val inRel = inRoot.relativize(it).toString()
                            if (inRel.endsWith(".class")) {
                                val className = inRel.substring(0, inRel.length - 6)
                                val classReader = ClassReader(Files.readAllBytes(it))
                                val classNode = ClassNode()
                                classReader.accept(classNode, 0)
                                classNodes[className] = classNode
                                val supers = linkedSetOf<String>()
                                val superName = classNode.superName
                                if (superName != null) supers.add(superName)
                                supers.addAll(classNode.interfaces)
                                superClasses[className] = supers
                            }
                        }
                    }
                }
            }
            Timer(version, "remapJarWrite").use {
                val classNames = superClasses.keys
                for (supers in superClasses.values) supers.retainAll(classNames)
                val remapper = AsmRemapper(mappings, superClasses, namespace)
                for ((className, classNode) in classNodes) {
                    val remappedNode = ClassNode()
                    val classRemapper = object : ClassRemapper(remappedNode, remapper) {


                    }
                    classNode.accept(classRemapper)
                    fixBridgeMethods(remappedNode)
                    renameLocalVariables(remappedNode)
                    val remappedName = remapper.map(className) ?: throw IllegalArgumentException("$className could not be remapped")
                    if (remappedNode.sourceFile == null) {
                        var end = remappedName.indexOf('$')
                        if (end < 0) end = remappedName.length
                        val start = remappedName.lastIndexOf('/', end) + 1
                        val name = remappedName.substring(start, end)
                        remappedNode.sourceFile = "$name.java"
                    }
                    val classWriter = ClassWriter(0)
                    remappedNode.accept(classWriter)
                    val outPath = outRoot.resolve("$remappedName.class")
                    Files.createDirectories(outPath.parent)
                    Files.write(outPath, classWriter.toByteArray())
                }
            }
        }
    }
}

class LocalVariableRenamer {
    private val names = HashSet<String>()
    private val nameCounts = HashMap<String, Int>()
    private val defaultLvNamePattern = Pattern.compile("^lvt\\d+$")

    fun process(methodNode: MethodNode) {
        if (methodNode.localVariables == null) {
            return
        }

        val toRename = mutableListOf<LocalVariableNode>()
        val isStatic = methodNode.access and Opcodes.ACC_STATIC != 0
        for (lv in methodNode.localVariables) {
            if (!isStatic && lv.index == 0) {
                continue  // this
            }
            if (defaultLvNamePattern.matcher(lv.name).matches()) {
                toRename.add(lv)
            } else {
                names.add(lv.name)
                nameCounts[lv.name] = 1
            }
        }

        for (lv in toRename) {
            lv.name = generateNameFor(lv.desc, lv.name)
        }
    }

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
                incrementLetter = false
                var className = type.substring(minOf(0, type.length))  // remove the 'L' prefix
                className = className.substringAfterLast('/')
                className = className.substringAfterLast('$')
                className = className.substring(0, maxOf(className.length - 1, 0))  // remove the ';' suffix

                if (className.isEmpty()) {
                    output("lv-rename", "bad className, desc=$desc, className=$className")
                    return@branch null
                }

                val varName = className[0].lowercaseChar() + className.substring(1)
                if (varName != className && isValidJavaIdentifier(varName)) {
                    varName
                } else {
                    // TODO: FIX "bad varName, desc=Lnet/minecraft/world/level/GameRules$1;, className=1, varName=1"
                    output("lv-rename", "bad varName, desc=$desc, className=$className, varName=$varName")
                    null
                }
            }
            else -> {
                output("lv-rename", "bad type, type=$type")
                null
            }
        }?.let {
            VarName(it, incrementLetter)
        }
    }

    private fun generateNameFor(desc: String, fallback: String): String {
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

fun renameLocalVariables(remappedNode: ClassNode) {
    for (methodNode in remappedNode.methods) {
        LocalVariableRenamer().process(methodNode)
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
package de.skyrising.guardian.gen.mappings

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import net.fabricmc.fernflower.api.IFabricJavadocProvider
import org.jetbrains.java.decompiler.struct.StructClass
import org.jetbrains.java.decompiler.struct.StructField
import org.jetbrains.java.decompiler.struct.StructMethod
import java.nio.file.Files
import java.nio.file.Path

data class JavadocMethodParameterData(val name: String, val comment: String)
data class JavadocMethodData(val comment: String?, val parameters: List<JavadocMethodParameterData>)
data class JavadocClassData(val comment: String?, val fields: Map<String, String>, val methods: Map<String, JavadocMethodData>)

class JavadocData : LinkedHashMap<String, JavadocClassData>()

// A holder class, to prevent accidental class loading
class FabricJavadocProviderCreator {
    companion object {
        fun createFabricJavadocProviderFromJson(path: Path?): Any {
            class JavadocProvider(private val data: JavadocData) {
                private val ACC_STATIC = 0x0008
                private val ACC_RECORD = 0x10000

                private fun isStatic(structClass: StructClass): Boolean {
                    return (structClass.accessFlags and ACC_STATIC) != 0
                }

                private fun isRecord(structClass: StructClass): Boolean {
                    return (structClass.accessFlags and ACC_RECORD) != 0
                }

                fun getClassDoc(clazz: StructClass): String? {
                    val classData = data[clazz.qualifiedName]
                    val lines = mutableListOf<String>()
                    if (classData?.comment != null) {
                        lines.add(classData.comment)
                    }
                    if (isRecord(clazz)) {
                        // insert record components docs to here
                        var needsNewLine = lines.size > 0
                        clazz.recordComponents.forEach { comp ->
                            val comment = classData?.fields?.get(MemberDescriptor(comp.name, comp.descriptor).toString())
                            if (comment != null) {
                                if (needsNewLine) {
                                    needsNewLine = false
                                    lines.add("")
                                }
                                lines.add("@param %s %s".format(comp.name, comment))
                            }
                        }
                    }
                    return if (lines.size > 0) {
                        lines.joinToString("\n")
                    } else {
                        null
                    }
                }

                fun getFieldDoc(clazz: StructClass, field: StructField): String? {
                    if (isRecord(clazz) && !isStatic(clazz)) {
                        return null  // handled in getClassDoc()
                    }
                    return data[clazz.qualifiedName]?.fields?.get(MemberDescriptor(field.name, field.descriptor).toString())
                }

                fun getMethodDoc(clazz: StructClass, method: StructMethod): String? {
                    val methodData = data[clazz.qualifiedName]?.methods?.get(MemberDescriptor(method.name, method.descriptor).toString()) ?: return null

                    val lines = mutableListOf<String>()
                    if (methodData.comment != null) {
                        lines.add(methodData.comment)
                    }

                    var needsNewLine = lines.size > 0
                    methodData.parameters.forEach { paramData ->
                        if (needsNewLine) {
                            needsNewLine = false
                            lines.add("")
                        }
                        lines.add("@param %s %s".format(paramData.name, paramData.comment))
                    }

                    return if (lines.size > 0) {
                        lines.joinToString("\n")
                    } else {
                        null
                    }
                }
            }

            val javadocProvider = if (path != null) {
                val data = Files.newBufferedReader(path).use { reader ->
                    return@use Gson().fromJson(reader, JavadocData::class.java)
                }
                JavadocProvider(data)
            } else {
                null
            }

            class IFabricJavadocProviderImpl: IFabricJavadocProvider {
                override fun getClassDoc(structClass: StructClass?): String? {
                    if (structClass != null) {
                        return javadocProvider?.getClassDoc(structClass)
                    }
                    return null
                }

                override fun getFieldDoc(structClass: StructClass?, structField: StructField?): String? {
                    if (structClass != null && structField != null) {
                        return javadocProvider?.getFieldDoc(structClass, structField)
                    }
                    return null
                }

                override fun getMethodDoc(structClass: StructClass?, structMethod: StructMethod?): String? {
                    if (structClass != null && structMethod != null) {
                        return javadocProvider?.getMethodDoc(structClass, structMethod)
                    }
                    return null
                }
            }

            return IFabricJavadocProviderImpl()
        }
    }
}

class MappingTreeJavadocDumper(private val classes: IndexedMemberList<String, ClassMapping>) {
    fun dumpToJsonFile(file: Path) {
        val data = mutableMapOf<String, Any>()
        classes.forEach { cm ->
            val fields = mutableMapOf<String, String>()
            cm.fields.forEach { fm ->
                if (fm.comment != null) {
                    fields[fm.defaultName.toString()] = fm.comment!!
                }
            }
            val methods = mutableMapOf<String, JavadocMethodData>()
            cm.methods.forEach { mm ->
                val parameters = mutableListOf<JavadocMethodParameterData>()
                mm.parameters.values.forEach{ pm ->
                    if (pm.comment != null) {
                        parameters.add(JavadocMethodParameterData(pm.name, pm.comment!!))
                    }
                }
                if (mm.comment != null || parameters.isNotEmpty()) {
                    methods[mm.defaultName.toString()] = JavadocMethodData(mm.comment, parameters)
                }
            }

            if (cm.comment == null && fields.isEmpty() && methods.isEmpty()) {
                return@forEach
            }

            val cls = mutableMapOf<String, Any?>()
            cls["comment"] = cm.comment
            cls["fields"] = fields
            cls["methods"] = methods
            data[cm.defaultName] = cls
        }

        Files.newBufferedWriter(file).use { writer ->
            val gson = GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()
            gson.toJson(data, writer)
        }
    }
}

fun dumpCommentFromYarnMappingTree(mappingTree: MappingTree, namespace: String, file: Path) {
    val classes = mappingTree.invert(namespace).classes
    MappingTreeJavadocDumper(classes).dumpToJsonFile(file)
}

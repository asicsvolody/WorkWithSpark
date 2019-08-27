package ru.yakimov.SparkAPI.WorkWhithFiles.TaskKompToAvro;

import javax.tools.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class StringCompilation {

    public static void writeArrToClass(String[] javaLinesArr, String pathTo){
        try(FileWriter writer = new FileWriter(new File(pathTo), true)) {
            for (String str : javaLinesArr) {
                if(!str.startsWith("package"))
                    writer.write(str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String[] printData(String classPath){
        List<String> list = new ArrayList<>();
        try(Scanner sc = new Scanner(new File(classPath))) {
            while (sc.hasNext()) {
                list.add(sc.nextLine()+"\n");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return list.toArray(new String[0]);
    }

    public static void compile(final File file) {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        final StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);

        List<String> optionList = new ArrayList<>();
        optionList.add("-classpath");
        optionList.add(System.getProperty("java.class.path") + System.getProperty("path.separator") + "/log4j-1.2.17.jar");

        Iterable<? extends JavaFileObject> compilationUnit
                = fileManager.getJavaFileObjectsFromFiles(Collections.singletonList(file));
        JavaCompiler.CompilationTask task = compiler.getTask(
                null,
                fileManager,
                diagnostics,
                optionList,
                null,
                compilationUnit);
        final boolean result = task.call();
        if (result) {
            System.out.println("Compilation was successful");
        } else {
            System.out.println("Compilation failed");
            for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {
                System.err.format("Error on line %d in %s", diagnostic.getLineNumber(), diagnostic);
            }
        }
        try {
            fileManager.close();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

}

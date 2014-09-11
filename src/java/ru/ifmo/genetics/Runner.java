package ru.ifmo.genetics;

import org.apache.log4j.Logger;
import ru.ifmo.genetics.tools.TranscriptomeAssembler;
import ru.ifmo.genetics.utils.tool.Tool;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static ru.ifmo.genetics.utils.TextUtils.fit;

public class Runner {
    private static final String USED_FS = "/";
    private static final String TOOLS_PACKAGE = "ru/ifmo/genetics/tools";
    private static final String RUNNER_PATH = "ru/ifmo/genetics/Runner.class";


    Tool defaultTool = new TranscriptomeAssembler();
    Logger logger = Logger.getLogger(Runner.class);
    
    PrintStream out = System.out;

    public void run(String[] args) {
        boolean printHelp = (args.length == 0 || args[0].equals("-h") || args[0].equals("--help"));
        boolean printVersion = (args.length > 0) && args[0].equals("--version");
        boolean printRunnerHelp = (args.length > 0) && (args[0].equals("-rh") || args[0].equals("--runner-help"));

        if (printVersion || printHelp || printRunnerHelp) {
            out.println("ITMO Genome Assembler, version " + getVersion());
            out.println();
        }
        if (printVersion) {
            System.exit(0);
        }
        
        if (printHelp || printRunnerHelp) {
            out.println("Usage: assembler [<JVM options>] [<Runner options>] [<Tool params>]");
            out.println();
            out.println("To see help for JVM options and Runner options type 'assembler -rh'.");
            out.println("To see help for a tool omit tool parameters.");
            out.println();
        }

        if (printRunnerHelp) {
            out.println("Java Virtual Machine (JVM) options:");
            out.println("\t" + fit("-m <memory-to-use>", 40) +
                    "set memory to use (optional, default: 95% of free memory for Linux, " +
                                                          "90% of free memory for Windows)");
            out.println("\t" + fit("-ea", 40) +
                    "enable assertions (optional, default: assertions disabled)");
            out.println();
            out.println("Runner options:");
            out.println("\t" + fit("-t, --tool-name <tool-name>", 40) +
                    "set certain tool to run (optional, default: " + defaultTool.name + ")");
            out.println("\t" + fit("-ts, --tools", 40) +
                    "print available tools (optional)");
            out.println();
            System.exit(0);
        }


        // processing runner options
        boolean printTools = (args.length > 0) && (args[0].equals("-ts") || args[0].equals("--tools"));
        boolean toolIsSet = (args.length > 0) && (args[0].equals("-t") || args[0].equals("--tool"));
        String toolName = null;
        if (toolIsSet) {
            if (args.length < 2) {
                throw new RuntimeException("Tool name isn't specified");
            }
            toolName = args[1];
            args = Arrays.copyOfRange(args, 2, args.length);
        }

        Tool tool = defaultTool;

        if (printTools || toolIsSet) {
            findTools();

            if (printTools) {
                out.println("Available tools:");
                out.println();
                for (Tool t : tools.values()) {
                    out.println(fit(t.name, 30) + t.description);
                }
                out.println();
                System.exit(0);
            }

            tool = tools.get(toolName);
            if (tool == null) {
                out.println("ERROR: Tool '" + toolName + "' not found");
                System.exit(1);
            }
        }

        if (printHelp) {
            out.println("Help for tool " + tool.name + ":");
        }


        // cleaning
        classes = null;
        tools = null;
        defaultTool = null;

        // running
        tool.mainImpl(args);
    }


    List<String> classes;
    Map<String, Tool> tools;

    private void findTools() {
        classes = new ArrayList<String>();
        tools = new TreeMap<String, Tool>();

        URL url = Runner.class.getResource("Runner.class");
//        out.println("url = " + url.toString());
        
        String protocol = url.getProtocol();
//        out.println("url.getProtocol = " + protocol);
        String path = url.getPath();
        path = path.substring(0, path.length() - RUNNER_PATH.length());
//        out.println("path = " + path);

        if (protocol.equals("file")) {
            File toolsDir = new File(path + TOOLS_PACKAGE);
//            out.println("tools dir = " + toolsDir);
            recursiveWalk(toolsDir);

        } else if (protocol.equals("jar")) {
            path = path.substring(5, path.length() - 2);    // removing 'file:' and '!/'
//            out.println("path = " + path);
            scanJarFile(path);

        } else {
            throw new RuntimeException("Unknown protocol " + protocol + ", can't scan files");
        }

//        out.println("classes.size = " + classes.size());
//        out.println("classes[0] = " + classes.get(0));

        identifyTools();
//        out.println("tools found = " + tools.size());
    }


    private void recursiveWalk(File dir) {
        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                recursiveWalk(file);
            } else {
                String fileName = file.getAbsolutePath();
                fileName = fileName.replace(File.separator, USED_FS);
                if (isGoodClassFile(fileName)) {
                    classes.add(fileName);
                }
            }
        }
    }
    
    private void scanJarFile(String jarFileName) {
        try {
            ZipFile jarFile = new ZipFile(new File(jarFileName));
            Enumeration<? extends ZipEntry> entries = jarFile.entries();

            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                String fileName = entry.getName();
                if (fileName.startsWith(TOOLS_PACKAGE) && isGoodClassFile(fileName)) {
                    classes.add(fileName);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isGoodClassFile(String fileName) {
        int li = fileName.lastIndexOf(USED_FS);
        fileName = (li != -1) ? fileName.substring(li + 1) : fileName;
        int pi = fileName.lastIndexOf('.');
        String type = (pi != -1) ? fileName.substring(pi + 1) : "";
        return type.equals("class") && !fileName.contains("$");
    }

    private void identifyTools() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        for (String classFileName : classes) {
            String className = getClassName(classFileName);
            Class clazz;
            try {
                clazz = cl.loadClass(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            if (Tool.class.isAssignableFrom(clazz) && (getMainMethod(clazz) != null)) {
                // clazz extends Tool and has main method
                Tool toolClass = null;
                try {
                    toolClass = (Tool) clazz.newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                
                assert !tools.containsKey(toolClass.name) :
                        "Already have a tool with name " + toolClass.name + ": " +
                                className + " and " + tools.get(toolClass.name).getClass().getName();
                tools.put(toolClass.name, toolClass);
            }
        }
    }

    private Method getMainMethod(Class clazz) {
        Method main = null;
        try {
            main = clazz.getMethod("main", String[].class);
        } catch (NoSuchMethodException e) {
        }
        return main;
    }

    private String getClassName(String fileName) {
        int si = fileName.indexOf(TOOLS_PACKAGE);
        if (si == -1) {
            throw new RuntimeException("si = -1");
        }
        String className = fileName.substring(si);

        int pi = className.lastIndexOf('.');
        className = className.substring(0, pi);     // removing ".class"
        className = className.replaceAll(USED_FS, ".");

        return className;
    }
    
    public static String getVersion() {
        InputStream versionFileStream = ClassLoader.getSystemResourceAsStream("VERSION");
        if (versionFileStream == null) {
            return "";
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(versionFileStream));
        String version;
        try {
            version = reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException("I/O error occurs while reading VERSION file");
        }
        return version;
    }
    
    public static String getVersionNumber() {
        String version = getVersion();
        int si = version.indexOf(' ');
        if (si == -1) {
            return version;
        }
        return version.substring(0, si);
    }
    
    

    public static void main(String[] args) {
        new Runner().run(args);
    }
}
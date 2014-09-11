package ru.ifmo.genetics.utils.tool;

import org.apache.commons.cli.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.*;
import org.jetbrains.annotations.NotNull;
import ru.ifmo.genetics.Runner;
import ru.ifmo.genetics.statistics.Timer;
import ru.ifmo.genetics.utils.Misc;
import ru.ifmo.genetics.utils.TextUtils;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.BoolParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.IntParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.ParameterBuilder;
import ru.ifmo.genetics.utils.tool.inputParameterBuilder.StringParameterBuilder;
import ru.ifmo.genetics.utils.tool.parameters.OutputParameter;
import ru.ifmo.genetics.utils.tool.parameters.ParameterDescription;
import ru.ifmo.genetics.utils.tool.values.*;

import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class Tool {
    private static final String IN_PARAM_FILE = "in.properties";
    private static final String OUT_PARAM_FILE = "out.properties";
    private static final String SUCCESS_FILE = "SUCCESS";


    private static final Logger mainLogger = Logger.getLogger("MAIN");
    protected final Logger logger;

    @NotNull public final String name;
    @NotNull public final String description;

    protected Tool(@NotNull String name, @NotNull String description) {
        this.name = name;
        this.description = description;
        logger = Logger.getLogger(name);
    }


    // ========================== parameters =============================

    // global input parameters, they are set in mainImpl()
    private static final List<Parameter> globalInputParameters = new ArrayList<Parameter>();

    private static final Parameter<String> workDirParameter = new Parameter<String>(new StringParameterBuilder("work-dir")
            .withShortOpt("w")
            .withDefaultValue("workDir")
            .withDescription("working directory")
            .create());

    private static final Parameter<Integer> availableProcessorsParameter = new Parameter<Integer>(new IntParameterBuilder("available-processors")
            .withShortOpt("p")
            .withDefaultValue(Runtime.getRuntime().availableProcessors())
            .withDefaultComment("all (" + Runtime.getRuntime().availableProcessors() + ")")
            .withDescription("available processors")
            .create());


    private static final Parameter<Boolean> continueParameter = new Parameter<Boolean>(new BoolParameterBuilder("continue")
            .withShortOpt("c")
            .withDescription("continue the previous run (there is no need to set other input parameters)")
            .create());

    private static final Parameter<Boolean> forceParameter = new Parameter<Boolean>(new BoolParameterBuilder("force")
            .withDescription("force run with rewriting old results")
            .create());

    private static final Parameter<String> startParameter = new Parameter<String>(new StringParameterBuilder("start")
            .withShortOpt("s")
            .withDescription("first force run stage (with rewriting old results)")
            .create());
    private static final Parameter<String> finishParameter = new Parameter<String>(new StringParameterBuilder("finish")
            .withShortOpt("f")
            .withDescription("finishing stage")
            .create());


    private static final Parameter<Boolean> helpParameter = new Parameter<Boolean>(new BoolParameterBuilder("help")
            .withShortOpt("h")
            .withDescription("print help message")
            .create());
//    private final Parameter<String> config = new ...
//    options.addOption("c", "config", true,  "sets the config file");

    private static final Parameter<Boolean> verboseParameter = new Parameter<Boolean>(new BoolParameterBuilder("verbose")
            .withShortOpt("v")
            .withDescription("enable debug output")
            .create());
    private static boolean verbose;


    static {
        globalInputParameters.add(workDirParameter);
        globalInputParameters.add(availableProcessorsParameter);
        globalInputParameters.add(continueParameter);
        globalInputParameters.add(forceParameter);
        globalInputParameters.add(startParameter);
        globalInputParameters.add(finishParameter);
        globalInputParameters.add(helpParameter);
        globalInputParameters.add(verboseParameter);
    }


    // personal (for tool) global input parameters
    public PathInValue workDir = new PathInValue(new FileInValue(workDirParameter));
    public Parameter<Integer> availableProcessors = new Parameter<Integer>(availableProcessorsParameter.description);
    {
        availableProcessors.set(availableProcessorsParameter);
    }


    // input and output parameters
    protected final List<Parameter> inputParameters = new ArrayList<Parameter>();
    protected final List<String> inputParameterToolNames = new ArrayList<String>();

    protected <T> Parameter<T> addParameter(ParameterDescription<T> description) {
        assert checkParameterNameUniqueness(description);
        
        Parameter<T> p = Parameter.createParameter(description);

        inputParameters.add(p);
        inputParameterToolNames.add(name);
        return p;
    }

    protected <T extends Tool> T addSubTool(T subTool) {
        assert checkToolNameUniqueness(subTool);
        
        for (Parameter subParameter: subTool.inputParameters) {
            ParameterBuilder builder = ParameterBuilder.createBuilder(subParameter.description);

            if (subParameter.internalValue != null) {
                if (subParameter.internalValue instanceof FixingInValue) {
                    continue;
                }

                builder.optional();
                //noinspection unchecked
                builder.withDefaultValue(subParameter.internalValue);
            }

            // checking for name uniqueness
            for (int i = 0; i < inputParameters.size(); i++) {
                Parameter c = inputParameters.get(i);
                if (c.description.name.equals(subParameter.description.name)) {
                    // change name of parameter c
                    ParameterBuilder cBuilder = new ParameterBuilder(c.description);
                    String toolName = inputParameterToolNames.get(i);
                    cBuilder.withName(toolName + "." + c.description.name);
                    //noinspection unchecked
                    c.replaceDescription(cBuilder.create());

                    // change name of subParameter
                    builder.withName(subTool.name + "." + subParameter.description.name);
                }
            }

            //noinspection unchecked
            subParameter.replaceDescription(builder.create());

            assert checkParameterNameUniqueness(subParameter.description);
            inputParameters.add(subParameter);
            inputParameterToolNames.add(subTool.name);
        }

        setWorkDirInSubTool(subTool);
        return subTool;
    }
    
    protected void setWorkDirInSubTool(Tool subTool) {
        subTool.workDir.set(workDir, subTool.name);
    }

    private boolean checkParameterNameUniqueness(ParameterDescription description) {
        for (Parameter parameter : inputParameters) {
            checkParameterNameUniqueness(parameter, description);
        }
        for (Parameter parameter : globalInputParameters) {
            checkParameterNameUniqueness(parameter, description);
        }
        return true;
    }

    private void checkParameterNameUniqueness(Parameter parameter, ParameterDescription description) {
        assert !parameter.description.name.equals(description.name) :
                "Two different parameters have the same name '" + description.name + "' " +
                        "in tool " + name;
        if (description.shortOpt != null && parameter.description.shortOpt != null) {
            assert !parameter.description.shortOpt.equals(description.shortOpt) :
                    "Parameters " + parameter.description.name + " and " + description.name +
                    " have the same short option '" + description.shortOpt + "' " +
                    "in tool " + name;
        }
    }
    
    private boolean checkToolNameUniqueness(Tool subTool) {
        for (String name : inputParameterToolNames) {
            assert !name.equals(subTool.name) :
                    "Two different steps have the same name '" + name + "' " +
                            "in tool " + this.name;
        }
        return true;
    }
    
    
    private final List<OutputParameter> outputParameters = new ArrayList<OutputParameter>();

    protected <T> InValue<T> addOutput(@NotNull String name, @NotNull InValue<T> output, Class<T> tClass) {
        OutputParameter<T> p = new OutputParameter<T>(name, output, tClass);
        outputParameters.add(p);
        return p;
    }


    protected abstract void clean() throws ExecutionFailedException;



    // ========================== run methods =============================

    protected abstract void runImpl() throws ExecutionFailedException;

    private void simpleRun(boolean forceRun, String start, String finish) throws ExecutionFailedException {
        assert checkArguments();
        runImpl();
        runAllSteps(forceRun, start, finish);
        clean();
    }

    public void simpleRun() throws ExecutionFailedException {
        simpleRun(true, null, null);
    }


    /**
     * Runs tool as one step with creating folders, dumping input and output params,
     *   creating SUCCESS file if run was successful.
     *
     * If forseRun is set then all data will be rewritten,
     *   else it will try to continue old run, if it is possible.
     *
     * @return true, if runAsStep called runImpl, else returns false (if SUCCESS file was found)
     */
    private boolean runAsStep(boolean forceRun, String start, String finish) throws ExecutionFailedException {
        File workDir = this.workDir.get();
        //noinspection ResultOfMethodCallIgnored
        workDir.mkdirs();

        File success = new File(workDir, SUCCESS_FILE);
        File inputParamFile = new File(workDir, IN_PARAM_FILE);
        File outputParamFile = new File(workDir, OUT_PARAM_FILE);


        forceRun |= !inputParamFile.exists();

        boolean shouldRun = forceRun || (start != null);

        boolean canLoadResult = inputParamFile.exists() && success.exists();
        if (!forceRun) {
            // trying to loading input parameters from previous run
            canLoadResult &= loadUnsetParametersFromProperties(inputParameters, inputParamFile);
        }
        shouldRun |= !canLoadResult;


        if (!shouldRun) {
            // loading result
            mainLogger.info("SUCCESS file found for tool " + name + " - loading results...");

            // input parameters was already loaded
            // loading output parameters
            loadOutputParametersFromProperties(outputParameters, outputParamFile);

            return false;

        } else {
            mainLogger.info("Running tool " + name);

            //noinspection ResultOfMethodCallIgnored
            success.delete();
            //noinspection ResultOfMethodCallIgnored
            outputParamFile.delete();


            dumpInputParameters(inputParameters, inputParamFile);

            // running
            simpleRun(forceRun, start, finish);
            if (finish != null) {
                return true;
            }


            dumpOutputParameters(outputParameters, outputParamFile);

            // creating file SUCCESS
            boolean created = false;
            try {
                created = success.createNewFile();
            } catch (IOException e) {
                throw new ExecutionFailedException(e);
            }
            assert created;

            return true;
        }
    }



    /**
     * Runs tool as one step with creating folders, dumping input and output params,
     *   creating SUCCESS file if run was successful.
     */
    public void run(boolean shouldContinue, boolean forceRun, String start, String finish) {
        // checking and processing --continue and --force options
        try {
            if (shouldContinue && forceRun) {
                throw new IllegalArgumentException("Continue and force options can't be set simultaneously");
            }

            File inputParamFile = new File(workDir.get(), IN_PARAM_FILE);
            if (inputParamFile.exists()) {
                if (shouldContinue) {
                    // OK
                } else if (forceRun) {
                    mainLogger.warn("Force run, all data in working directory will be rewritten!");
                } else {
                    System.err.print("Working directory (" + workDir.get() + "/) "+
                            "contains files from previous run, rewrite them? [Yes(y)/No(n), default:No] ");
                    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                    String ans = "";
                    try {
                        ans = in.readLine();
                    } catch (IOException e) {
                        throw new RuntimeException("I/O error occurs while reading answer");
                    }
                    if (TextUtils.isYes(ans)) {
                        forceRun = true;
                    } else {
                        System.exit(1);
                        return;
                    }
                }
            } else {
                // OK, no data will be rewritten
                forceRun = true;    // doesn't load anything
            }

            if (forceRun) {
                checkArguments();
            }

        } catch (IllegalArgumentException e) {
            mainLogger.error(e.getMessage(), e);
            System.exit(1);
            return;
        } catch (NotSetArgumentException e) {
            mainLogger.error(e.getMessage(), e);
            System.exit(1);
            return;
        }

        // running
        try {
            runAsStep(forceRun, start, finish);
        } catch (ExecutionFailedException e) {
            mainLogger.error(e.getMessage(), e);
            System.exit(1);
            return;
        }
    }
    
    /**
     * Runs tool with creating folders, dumping input and output params,
     *   creating SUCCESS file if run was successful.
     */
    public void run() {
        run(false, false, null, null);
    }


    private final Queue<Tool> steps = new LinkedBlockingQueue<Tool>();

    /**
     * Adds the step that will be executed in future.<br></br>
     * All steps will be executed after runImpl method finished.
     */
    public void addStep(Tool tool) {
        steps.add(tool);
    }

    private void runAllSteps(boolean forceRun, String start, String finish) throws ExecutionFailedException {
        // checking existence of start and finish tools' names
        checkBoundExistence(start);
        checkBoundExistence(finish);

        // running
        Tool tool = null;
        while (!steps.isEmpty()) {
            tool = steps.remove();

            if (tool.name.equals(start)) {
                forceRun = true;
            }

            String newStart = getNextBoundName(start, tool.name);
            String newFinish = getNextBoundName(finish, tool.name);

            forceRun |= tool.runAsStep(forceRun, newStart, newFinish);

            if (finish != null && finish.startsWith(tool.name)) {
                break;
            }
        }

        if (!steps.isEmpty()) {
            // we probably should remove next SUCCESS file
            Tool nextTool = steps.remove();
            if (finish != null && finish.equals(tool.name)) {
                // i.e. we finished all tool's substeps, and nextTool have outdated results
                File success = new File(nextTool.workDir.get(), SUCCESS_FILE);
                //noinspection ResultOfMethodCallIgnored
                success.delete();
                File inputParamFile = new File(nextTool.workDir.get(), IN_PARAM_FILE);
                //noinspection ResultOfMethodCallIgnored
                inputParamFile.delete();
            }
        }

        steps.clear();
    }



    // ========================== main implementation =============================

    /**
     * Always calls System.exit(return-code)
     *
     * @param args command line arguments
     */
    public void mainImpl(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            private Runtime runtime = Runtime.getRuntime();
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                try {
                    mainLogger.error("Uncaught exception: " + e, e);
                    runtime.exit(1);
                } catch (Throwable e1) {
                    System.err.println(e.getMessage());
                    runtime.exit(1);
                }
            }
        });

        // preparing
        List<Parameter> allParameters = new ArrayList<Parameter>();
        allParameters.addAll(inputParameters);
        allParameters.addAll(globalInputParameters);

        // parsing args
        Options options = new Options();
        for (Parameter p : allParameters) {
            options.addOption(p.getAsOption());
        }

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            mainLogger.error("cannot parse command line", e);
            System.exit(1);
            return;
        }

        if (cmd.hasOption(helpParameter.description.name) || (args.length == 0)) {
            printHelp();
            System.exit(0);
            return;
        }

        // loading config - config not used now
        /*
        Configuration config = new PropertiesConfiguration();

        String configFileName = cmd.getOptionValue("config");
        if (configFileName != null) {
            try {
                config = new PropertiesConfiguration(configFileName);
            } catch (ConfigurationException e) {
                mainLogger.error("cannot configure tool", e);
                System.exit(1);
                return;
            }
        }
        */

        // setting parameters
        for (Parameter p : allParameters) {
            String key = p.description.name;
//            Object value = config.getProperty(key);
            String[] value;
            if (cmd.hasOption(key)) {
                value = cmd.getOptionValues(key);
                if (value == null) {    // option doesn't have an argument
                    value = new String[]{ "true" };
                }
                parseAndSet(p, value, p.description.tClass, p.description.name);
            }
        }

        addFileLogger();

        // processing global input parameters
        verbose = verboseParameter.get();
        if (verbose) {
            ConsoleAppender ca = (ConsoleAppender) Logger.getRootLogger().getAppender("console");
            ca.setThreshold(Level.DEBUG);
        }
        boolean shouldContinue = (continueParameter.get() != null && continueParameter.get());
        boolean forceRun = (forceParameter.get() != null && forceParameter.get());

        mainLogger.debug("Assembler version = " + Runner.getVersion());
        mainLogger.debug("Assembler params = " + Arrays.toString(args));
        assert printAssertionsEnabled();
        mainLogger.debug("Available memory = " + Misc.availableMemoryAsString());


        // running main tool
        run(shouldContinue, forceRun, startParameter.get(), finishParameter.get());


        System.exit(0);
    }

    private void addFileLogger() {
        String timestamp = new SimpleDateFormat("yyyy.MM.dd_HH.mm.ss").format(new Date());
        String header = "Log created at " +
                new SimpleDateFormat("dd-MMM-yyyy (EEE) HH:mm:ss").format(new Date());

        File logsDir = workDir.append("logs").get();
        //noinspection ResultOfMethodCallIgnored
        workDir.get().mkdirs();
        //noinspection ResultOfMethodCallIgnored
        logsDir.mkdirs();

        String lastLogFile = workDir.append("log").toString();
        String logFile = logsDir + File.separator + "log_" + timestamp;

        addFileLogger(lastLogFile, header);
        addFileLogger(logFile, header);
    }
    
    private void addFileLogger(String logFile, String header) {
        // adding header to logs file
        try {
            PrintWriter pw = new PrintWriter(logFile);
            pw.println(header);
            pw.close();
        } catch (FileNotFoundException e) {
            mainLogger.warn("Can't create log file");
        }

        // adding file logger
        PatternLayout layout = new PatternLayout("%d{ISO8601} %p %c: %m%n");
        try {
            FileAppender fa = new FileAppender(layout, logFile);
            fa.activateOptions();
            Logger.getRootLogger().addAppender(fa);
        } catch (IOException e) {
            mainLogger.warn("Can't create log file appender");
        }
    }

    private boolean printAssertionsEnabled() {
        mainLogger.info("Assertion checking is enabled !");
        return true;
    }


    // ========================== private auxiliary methods =============================

    protected boolean checkArguments() {
        for (Parameter p : inputParameters) {
            if (p.description.mandatory && p.get() == null) {
                String shortOpt = "";
                if (p.description.shortOpt != null) {
                    shortOpt = " (-" + p.description.shortOpt + ")";
                }
                throw new NotSetArgumentException("Mandatory argument --" + p.description.name + shortOpt + " not set");
            }
        }
        return true;
    }

    private void checkBoundExistence(String bound) {
        if (bound == null) {
            return;
        }

        String firstName = getFirstBoundName(bound);
        boolean found = false;
        for (Tool tool : steps) {
            found |= tool.name.equals(firstName);
        }
        if (!found) {
            throw new IllegalArgumentException("There is no substep with name '" + firstName +
                    "' in step " + name + "!");
        }
    }


    /**
     * @return canLoadResult (true, if no parameter with different values (set and in properties) found)
     */
    private static boolean loadUnsetParametersFromProperties(List<Parameter> parameters, File propertiesFile) throws ExecutionFailedException {
        if (!propertiesFile.exists()) {
            return false;
        }

        boolean canLoadResult = true;
        PropertiesConfiguration properties = loadProperties(propertiesFile);

        for (Parameter parameter : parameters) {
            String key = parameter.description.name;
            Class tClass = parameter.description.tClass;

            String[] strPropValue = (tClass.isArray()) ? properties.getStringArray(key) : new String[]{properties.getString(key)};
            Object[] propValue = parse(strPropValue, tClass, key);

            if (parameter.get() == null) {
                // parameter not set
                set(parameter, propValue, tClass);
            } else {
                // parameter has a value
                boolean equals = checkEquals(parameter, propValue, tClass);
                canLoadResult &= equals;
                if (!equals) {
                    mainLogger.debug("Parameter " + parameter.description.name + " changed from last run");
                }
            }
        }

        return canLoadResult;
    }

    private static void loadOutputParametersFromProperties(List<OutputParameter> parameters, File propertiesFile) throws ExecutionFailedException {
        PropertiesConfiguration properties = loadProperties(propertiesFile);
        for (OutputParameter parameter : parameters) {
            String key = parameter.name;
            Class tClass = parameter.tClass;
            String[] value = (tClass.isArray()) ? properties.getStringArray(key) : new String[]{properties.getString(key)};
            parseAndSet(parameter, value, tClass, key);
        }
    }
    
    private static void dumpInputParameters(List<Parameter> parameters, File propertiesFile) throws ExecutionFailedException {
        PropertiesConfiguration inProperties = new PropertiesConfiguration();
        inProperties.setDelimiterParsingDisabled(true);
        for (Parameter parameter : parameters) {
            addProperty(inProperties, parameter, parameter.description.name);
        }
        dumpProperties(inProperties, propertiesFile);
    }
    
    private static void dumpOutputParameters(List<OutputParameter> parameters, File propertiesFile) throws ExecutionFailedException {
        PropertiesConfiguration outProperties = new PropertiesConfiguration();
        outProperties.setDelimiterParsingDisabled(true);
        for (OutputParameter parameter : parameters) {
            addProperty(outProperties, parameter, parameter.name);
        }
        dumpProperties(outProperties, propertiesFile);
    }


    private static void parseAndSet(OutValue parameter, String[] strValue, Class tClass, String parameterName) {
        Object[] value = parse(strValue, tClass, parameterName);
        set(parameter, value, tClass);
    }

    private static void set(OutValue parameter, Object[] value, Class tClass) {
        if (tClass.isArray()) {
            //noinspection unchecked
            parameter.set(value);
        } else {
            //noinspection unchecked
            parameter.set(value[0]);
        }
    }
    
    private static Object[] parse(String[] value, Class tClass, String parameterName) {
        if (value == null) {
            return null;
        }
        if (value.length == 0 || value[0] == null) {
            return (tClass.isArray()) ? new Object[]{} : new Object[]{null};
        }

        if (tClass.isArray()) {
            Class tElementClass = tClass.getComponentType();

            // converting
            Object[] newValue = (Object[]) Array.newInstance(tElementClass, value.length);
            for (int i = 0; i < value.length; i++) {
                try {
                    newValue[i] = convert(value[i], tElementClass);
                } catch (Exception e) {
                    mainLogger.error("Can't convert value '" + value[i] + "' of parameter '" + parameterName + "' to type '" + tClass + "'", e);
                    System.exit(1);
                }
            }

            return newValue;

        } else {
            // converting
            Object newValue = null;
            try {
                newValue = convert(value[0], tClass);
            } catch (Exception e) {
                mainLogger.error("Can't convert value '" + value[0] + "' of parameter '" + parameterName + "' to type '" + tClass + "'", e);
                System.exit(1);
            }

            return new Object[]{newValue};
        }
    }
    
    private static Object convert(Object value, Class tClass) throws Exception {
        //noinspection unchecked
        Constructor constr = tClass.getConstructor(value.getClass());
        value = constr.newInstance(value);
        return value;
    }
    
    private static Object parseArray(String s) {
        StringTokenizer st = new StringTokenizer(s, "[, ]");
        String[] array = new String[st.countTokens()];
        for (int i = 0; i < array.length; i++) {
            array[i] = st.nextToken();
        }
        return array;
    }


    private static boolean checkEquals(InValue parameter, Object[] value, Class tClass) {
        boolean res;
        if (tClass.isArray()) {
            res = Misc.checkEquals((Object[]) parameter.get(), value);
        } else {
            res = Misc.checkEquals(parameter.get(), value[0]);
        }
        return res;
    }



    private static PropertiesConfiguration loadProperties(File propertiesFile) throws ExecutionFailedException {
        try {
            PropertiesConfiguration res = new PropertiesConfiguration();
            res.setDelimiterParsingDisabled(true);
            Reader r = new BufferedReader(new FileReader(propertiesFile));
            res.load(r);
            return res;
        } catch (ConfigurationException e) {
            throw new ExecutionFailedException("Can't load properties from " + propertiesFile, e);
        } catch (FileNotFoundException e) {
            throw new ExecutionFailedException("File '" + propertiesFile + "'" + " with properties not found", e);
        }
    }

    private static void dumpProperties(PropertiesConfiguration config, File propertiesFile) throws ExecutionFailedException {
        PrintWriter out;
        try {
            out = new PrintWriter(propertiesFile);
            config.save(out);
            out.close();
        } catch (IOException e) {
            throw new ExecutionFailedException("Can't dump configuration", e);
        } catch (ConfigurationException e) {
            throw new ExecutionFailedException("Can't dump configuration", e);
        }
    }

    private static void addProperty(Configuration properties, InValue parameter, String name) {
        Object value = parameter.get();
        if (value != null) {
            if (value.getClass().isArray()) {
                Object[] objArray = (Object[]) value;
                for (int i = 0; i < objArray.length; i++) {
                    properties.addProperty(name, objectToString(objArray[i]));
                }
//                String[] x = properties.getStringArray(name);
//                mainLogger.debug("added property " + name + ": " + Arrays.toString(x) + " of length " + x.length);
            } else {
                properties.addProperty(name, objectToString(value));
            }
        }
    }

    private static String objectToString(Object propValue) {
        if (propValue instanceof File) {
            return ((File) propValue).getAbsolutePath();
        }
        if (propValue instanceof PathInValue) {
            return ((PathInValue) propValue).get().getAbsolutePath();
        }

        /*
        if (propValue.getClass().isArray()) {
            Object[] x = (Object[]) propValue;
            return x.length + ":" + Arrays.toString(x);
        }
        */

        return propValue.toString();
    }


    private static String getFirstBoundName(String curBound) {
        if (curBound != null) {
            int i = curBound.indexOf('.');
            if (i != -1) {
                curBound = curBound.substring(0, i);
            }
            return curBound;
        }
        return null;
    }

    private static String getNextBoundName(String curBound, String subToolName) {
        if (curBound != null && curBound.startsWith(subToolName)) {
            if (curBound.length() == subToolName.length()) {
                return null;
            }
            return curBound.substring(subToolName.length() + 1);
        }
        return null;
    }




    // ========================== help, config =============================

    public void printHelp() {
        System.out.println("Tool:           " + name);
        System.out.println("Description:    " + description);
        System.out.println("Parameters:");

        for (Parameter p : inputParameters) {
            String dh = p.description.printHelp();
            System.out.println("\t" + dh);
        }
        
        System.out.println();
        System.out.println("Global options:");
        for (Parameter p : globalInputParameters) {
            String dh = p.description.printHelp();
            System.out.println("\t" + dh);
        }
    }

    public void printConfig() {
        // TODO: ReWrite it
        String pack = getClass().getPackage().getName();
        String name = getClass().getSimpleName();
        System.out.println("// Package " + pack);
        System.out.println("// Class " + name);

        for (Parameter p : inputParameters) {
            String c = p.description.printConfig();
            if (c != null) {
                System.out.println(name + "." + c);
            }
        }
        
        System.out.println();
    }



    // ========================== simplified set methods =============================

    protected static <T> void setFix(Parameter<T> parameter, InValue<T> valueIn) {
        parameter.set(new ForwardingFixingInValue<T>(valueIn));
    }

    protected static <T> void setFix(Parameter<T> parameter, T value) {
        parameter.set(new SimpleFixingInValue<T>(value));
    }

    protected static <T> void setDefault(Parameter<T> parameter, InValue<T> value) {
        assert !(value instanceof FixingInValue);
        parameter.set(value);
    }

    protected static <T> void setFixDefault(Parameter<T> parameter) {
        assert parameter.description.defaultValue != null;
        setFix(parameter, parameter.description.defaultValue);
    }




    // ========================== output methods =============================

    // logging
    protected void warn(Object message) {
        if (verbose) {
            clearProgressImpl();
            logger.warn(message);
            System.err.print(curProgress);
        } else {
            logger.warn(message);
        }
    }

    protected void info(Object message) {
        if (verbose) {
            clearProgressImpl();
            logger.info(message);
            System.err.print(curProgress);
        } else {
            logger.info(message);
        }
    }

    protected void debug(Object message) {
        if (verbose) {
            clearProgressImpl();
            logger.debug(message);
            System.err.print(curProgress);
        } else {
            logger.debug(message);
        }
    }


    // progress
    private static String curProgress = "";
    
    protected void showProgress(String progress) {
        clearProgress();
        debug(progress);
        System.err.print(progress);
        curProgress = progress;
    }
    
    public static void showProgressOnly(String progress) {
        clearProgress();
        System.err.print(progress);
        curProgress = progress;
    }

    protected void addProgress(String progress) {
        debug(progress);
        System.err.print(progress);
        curProgress += progress;
    }

    public static void addProgressOnly(String progress) {
        System.err.print(progress);
        curProgress += progress;
    }

    public static void clearProgress() {
        clearProgressImpl();
        curProgress = "";
    }

    private static final String WS_STRING = TextUtils.multiply(' ', 70);

    private static void clearProgressImpl() {
        System.err.print("\r");
        System.err.print(WS_STRING);
        System.err.print("\r");
    }


    // progress bar
    private static Timer timer = new Timer();
    private static long totalTasks;
    private static volatile long tasksDone;
    private static long updateMask;

    private static Thread progressWriterThread = null;
    private static Runnable progressWriter = new Runnable() {
        @Override
        public void run() {
            while (true) {
                double progress = (totalTasks == 0) ? 100 :
                        100.0 * tasksDone / totalTasks;
                String curProgress = "Progress: " + String.format("%.1f", progress) + "%";
                if (progress >= 0.1) {
                    long dt = timer.finish();
                    double dp = progress;
                    double rp = 100 - progress;
                    long rt = (long) (rp * (dt / dp));
                    curProgress += ", remaining time: " + Timer.timeToStringWithoutMs(rt);
                }
                
                showProgressOnly(curProgress);

                try {
                    Thread.sleep(1000);     // 1 second
                } catch (InterruptedException e) {
                    break;
                }
            }
            clearProgress();
        }
    };

    public static void createProgressBar(long totalTasks) {
        assert totalTasks >= 0;
        Tool.totalTasks = totalTasks;
        tasksDone = 0;

        long dv = Long.highestOneBit(totalTasks);
        dv >>= 10;    // ~0.1%
        updateMask = (dv == 0) ? 0 : (dv - 1);

        assert progressWriterThread == null;
        progressWriterThread = new Thread(progressWriter);
        progressWriterThread.start();
        timer.start();
    }

    public static void updateProgressBar(long tasksDone) {
        if ((tasksDone & updateMask) == 0) {
            Tool.tasksDone = tasksDone;
        }
    }

    public static void destroyProgressBar() {
        progressWriterThread.interrupt();
        try {
            progressWriterThread.join();    // waiting for thread to finish
        } catch (InterruptedException e) {
            mainLogger.debug("Interrupted in destroyProgressBar while waiting for progressWriterThread to finish");
        }
        progressWriterThread = null;
    }
}
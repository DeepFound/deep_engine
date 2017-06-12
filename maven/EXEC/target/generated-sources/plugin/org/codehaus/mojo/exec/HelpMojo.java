package org.codehaus.mojo.exec;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Display help information on maven-exec-plugin.<br/> Call <pre>  mvn exec:help -Ddetail=true -Dgoal=&lt;goal-name&gt;</pre> to display parameter details.
 *
 * @version generated on Sat Nov 26 14:22:07 EST 2016
 * @author org.apache.maven.tools.plugin.generator.PluginHelpGenerator (version 2.5.1)
 * @goal help
 * @requiresProject false
 */
public class HelpMojo
    extends AbstractMojo
{
    /**
     * If <code>true</code>, display all settable properties for each goal.
     * 
     * @parameter expression="${detail}" default-value="false"
     */
    private boolean detail;

    /**
     * The name of the goal for which to show help. If unspecified, all goals will be displayed.
     * 
     * @parameter expression="${goal}"
     */
    private java.lang.String goal;

    /**
     * The maximum length of a display line, should be positive.
     * 
     * @parameter expression="${lineLength}" default-value="80"
     */
    private int lineLength;

    /**
     * The number of spaces per indentation level, should be positive.
     * 
     * @parameter expression="${indentSize}" default-value="2"
     */
    private int indentSize;


    /** {@inheritDoc} */
    public void execute()
        throws MojoExecutionException
    {
        if ( lineLength <= 0 )
        {
            getLog().warn( "The parameter 'lineLength' should be positive, using '80' as default." );
            lineLength = 80;
        }
        if ( indentSize <= 0 )
        {
            getLog().warn( "The parameter 'indentSize' should be positive, using '2' as default." );
            indentSize = 2;
        }

        StringBuffer sb = new StringBuffer();

        append( sb, "com.cloudtree:maven-exec-plugin:1.0.0", 0 );
        append( sb, "", 0 );

        append( sb, "maven-exec-plugin", 0 );
        append( sb, "(no description available)", 1 );
        append( sb, "", 0 );

        if ( goal == null || goal.length() <= 0 )
        {
            append( sb, "This plugin has 3 goals:", 0 );
            append( sb, "", 0 );
        }

        if ( goal == null || goal.length() <= 0 || "exec".equals( goal ) )
        {
            append( sb, "exec:exec", 0 );
            append( sb, "A Plugin for executing external programs.", 1 );
            append( sb, "", 0 );
            if ( detail )
            {
                append( sb, "Available parameters:", 1 );
                append( sb, "", 0 );

                append( sb, "arguments", 2 );
                append( sb, "Can be of type <argument> or <classpath> Can be overriden using \'exec.args\' env. variable", 3 );
                append( sb, "", 0 );

                append( sb, "classpathScope (Default: runtime)", 2 );
                append( sb, "Defines the scope of the classpath passed to the plugin. Set to compile,test,runtime or system depending on your needs. Since 1.1.2, the default value is \'runtime\' instead of \'compile\'.", 3 );
                append( sb, "", 0 );

                append( sb, "commandlineArgs", 2 );
                append( sb, "Arguments for the executed program", 3 );
                append( sb, "", 0 );

                append( sb, "environmentVariables", 2 );
                append( sb, "Environment variables to pass to the executed program.", 3 );
                append( sb, "", 0 );

                append( sb, "executable", 2 );
                append( sb, "The executable. Can be a full path or a the name executable. In the latter case, the executable must be in the PATH for the execution to work.", 3 );
                append( sb, "", 0 );

                append( sb, "longClasspath (Default: false)", 2 );
                append( sb, "If set to true the classpath and the main class will be written to a MANIFEST.MF file and wrapped into a jar. Instead of \'-classpath/-cp CLASSPATH mainClass\' the exec plugin executes \'-jar maven-exec.jar\'.", 3 );
                append( sb, "", 0 );

                append( sb, "outputFile", 2 );
                append( sb, "Program standard and error output will be redirected to the file specified by this optional field. If not specified the standard maven logging is used.", 3 );
                append( sb, "", 0 );

                append( sb, "skip (Default: false)", 2 );
                append( sb, "Skip the execution.", 3 );
                append( sb, "", 0 );

                append( sb, "sourceRoot", 2 );
                append( sb, "This folder is added to the list of those folders containing source to be compiled. Use this if your plugin generates source code.", 3 );
                append( sb, "", 0 );

                append( sb, "successCodes", 2 );
                append( sb, "Exit codes to be resolved as successful execution for non-compliant applications (applications not returning 0 for success).", 3 );
                append( sb, "", 0 );

                append( sb, "testSourceRoot", 2 );
                append( sb, "This folder is added to the list of those folders containing source to be compiled for testing. Use this if your plugin generates test source code.", 3 );
                append( sb, "", 0 );

                append( sb, "workingDirectory", 2 );
                append( sb, "The current working directory. Optional. If not specified, basedir will be used.", 3 );
                append( sb, "", 0 );
            }
        }

        if ( goal == null || goal.length() <= 0 || "help".equals( goal ) )
        {
            append( sb, "exec:help", 0 );
            append( sb, "Display help information on maven-exec-plugin.\nCall\n\u00a0\u00a0mvn\u00a0exec:help\u00a0-Ddetail=true\u00a0-Dgoal=<goal-name>\nto display parameter details.", 1 );
            append( sb, "", 0 );
            if ( detail )
            {
                append( sb, "Available parameters:", 1 );
                append( sb, "", 0 );

                append( sb, "detail (Default: false)", 2 );
                append( sb, "If true, display all settable properties for each goal.", 3 );
                append( sb, "", 0 );

                append( sb, "goal", 2 );
                append( sb, "The name of the goal for which to show help. If unspecified, all goals will be displayed.", 3 );
                append( sb, "", 0 );

                append( sb, "indentSize (Default: 2)", 2 );
                append( sb, "The number of spaces per indentation level, should be positive.", 3 );
                append( sb, "", 0 );

                append( sb, "lineLength (Default: 80)", 2 );
                append( sb, "The maximum length of a display line, should be positive.", 3 );
                append( sb, "", 0 );
            }
        }

        if ( goal == null || goal.length() <= 0 || "java".equals( goal ) )
        {
            append( sb, "exec:java", 0 );
            append( sb, "Executes the supplied java class in the current VM with the enclosing project\'s dependencies as classpath.", 1 );
            append( sb, "", 0 );
            if ( detail )
            {
                append( sb, "Available parameters:", 1 );
                append( sb, "", 0 );

                append( sb, "arguments", 2 );
                append( sb, "The class arguments.", 3 );
                append( sb, "", 0 );

                append( sb, "classpathScope (Default: runtime)", 2 );
                append( sb, "Defines the scope of the classpath passed to the plugin. Set to compile,test,runtime or system depending on your needs. Since 1.1.2, the default value is \'runtime\' instead of \'compile\'.", 3 );
                append( sb, "", 0 );

                append( sb, "cleanupDaemonThreads", 2 );
                append( sb, "Wether to interrupt/join and possibly stop the daemon threads upon quitting.\nIf this is false, maven does nothing about the daemon threads. When maven has no more work to do, the VM will normally terminate any remaining daemon threads.\nIn certain cases (in particular if maven is embedded), you might need to keep this enabled to make sure threads are properly cleaned up to ensure they don\'t interfere with subsequent activity. In that case, see daemonThreadJoinTimeout and stopUnresponsiveDaemonThreads for further tuning.\n", 3 );
                append( sb, "", 0 );

                append( sb, "commandlineArgs", 2 );
                append( sb, "Arguments for the executed program", 3 );
                append( sb, "", 0 );

                append( sb, "daemonThreadJoinTimeout (Default: 15000)", 2 );
                append( sb, "This defines the number of milliseconds to wait for daemon threads to quit following their interruption.\nThis is only taken into account if cleanupDaemonThreads is true. A value <=0 means to not timeout (i.e. wait indefinitely for threads to finish). Following a timeout, a warning will be logged.\nNote: properly coded threads should terminate upon interruption but some threads may prove problematic: as the VM does interrupt daemon threads, some code may not have been written to handle interruption properly. For example java.util.Timer is known to not handle interruptions in JDK <= 1.6. So it is not possible for us to infinitely wait by default otherwise maven could hang. A sensible default value has been chosen, but this default value may change in the future based on user feedback.\n", 3 );
                append( sb, "", 0 );

                append( sb, "executableDependency", 2 );
                append( sb, "If provided the ExecutableDependency identifies which of the plugin dependencies contains the executable class. This will have the affect of only including plugin dependencies required by the identified ExecutableDependency.\nIf includeProjectDependencies is set to true, all of the project dependencies will be included on the executable\'s classpath. Whether a particular project dependency is a dependency of the identified ExecutableDependency will be irrelevant to its inclusion in the classpath.\n", 3 );
                append( sb, "", 0 );

                append( sb, "includePluginDependencies (Default: false)", 2 );
                append( sb, "Indicates if this plugin\'s dependencies should be used when executing the main class.\nThis is useful when project dependencies are not appropriate. Using only the plugin dependencies can be particularly useful when the project is not a java project. For example a mvn project using the csharp plugins only expects to see dotnet libraries as dependencies.\n", 3 );
                append( sb, "", 0 );

                append( sb, "includeProjectDependencies (Default: true)", 2 );
                append( sb, "Indicates if the project dependencies should be used when executing the main class.", 3 );
                append( sb, "", 0 );

                append( sb, "keepAlive (Default: false)", 2 );
                append( sb, "Deprecated. since 1.1-alpha-1", 3 );
                append( sb, "", 0 );
                append( sb, "Indicates if mojo should be kept running after the mainclass terminates. Usefull for serverlike apps with deamonthreads.", 3 );
                append( sb, "", 0 );

                append( sb, "killAfter (Default: -1)", 2 );
                append( sb, "Deprecated. since 1.1-alpha-1", 3 );
                append( sb, "", 0 );
                append( sb, "Deprecated this is not needed anymore.", 3 );
                append( sb, "", 0 );

                append( sb, "mainClass", 2 );
                append( sb, "The main class to execute.", 3 );
                append( sb, "", 0 );

                append( sb, "skip (Default: false)", 2 );
                append( sb, "Skip the execution.", 3 );
                append( sb, "", 0 );

                append( sb, "sourceRoot", 2 );
                append( sb, "This folder is added to the list of those folders containing source to be compiled. Use this if your plugin generates source code.", 3 );
                append( sb, "", 0 );

                append( sb, "stopUnresponsiveDaemonThreads", 2 );
                append( sb, "Wether to call Thread.stop() following a timing out of waiting for an interrupted thread to finish. This is only taken into account if cleanupDaemonThreads is true and the daemonThreadJoinTimeout threshold has been reached for an uncooperative thread. If this is false, or if Thread.stop() fails to get the thread to stop, then a warning is logged and Maven will continue on while the affected threads (and related objects in memory) linger on. Consider setting this to true if you are invoking problematic code that you can\'t fix. An example is Timer which doesn\'t respond to interruption. To have Timer fixed, vote for this bug.", 3 );
                append( sb, "", 0 );

                append( sb, "systemProperties", 2 );
                append( sb, "A list of system properties to be passed. Note: as the execution is not forked, some system properties required by the JVM cannot be passed here. Use MAVEN_OPTS or the exec:exec instead. See the user guide for more information.", 3 );
                append( sb, "", 0 );

                append( sb, "testSourceRoot", 2 );
                append( sb, "This folder is added to the list of those folders containing source to be compiled for testing. Use this if your plugin generates test source code.", 3 );
                append( sb, "", 0 );
            }
        }

        if ( getLog().isInfoEnabled() )
        {
            getLog().info( sb.toString() );
        }
    }

    /**
     * <p>Repeat a String <code>n</code> times to form a new string.</p>
     *
     * @param str String to repeat
     * @param repeat number of times to repeat str
     * @return String with repeated String
     * @throws NegativeArraySizeException if <code>repeat < 0</code>
     * @throws NullPointerException if str is <code>null</code>
     */
    private static String repeat( String str, int repeat )
    {
        StringBuffer buffer = new StringBuffer( repeat * str.length() );

        for ( int i = 0; i < repeat; i++ )
        {
            buffer.append( str );
        }

        return buffer.toString();
    }

    /** 
     * Append a description to the buffer by respecting the indentSize and lineLength parameters.
     * <b>Note</b>: The last character is always a new line.
     * 
     * @param sb The buffer to append the description, not <code>null</code>.
     * @param description The description, not <code>null</code>.
     * @param indent The base indentation level of each line, must not be negative.
     */
    private void append( StringBuffer sb, String description, int indent )
    {
        for ( Iterator it = toLines( description, indent, indentSize, lineLength ).iterator(); it.hasNext(); )
        {
            sb.append( it.next().toString() ).append( '\n' );
        }
    }

    /** 
     * Splits the specified text into lines of convenient display length.
     * 
     * @param text The text to split into lines, must not be <code>null</code>.
     * @param indent The base indentation level of each line, must not be negative.
     * @param indentSize The size of each indentation, must not be negative.
     * @param lineLength The length of the line, must not be negative.
     * @return The sequence of display lines, never <code>null</code>.
     * @throws NegativeArraySizeException if <code>indent < 0</code>
     */
    private static List toLines( String text, int indent, int indentSize, int lineLength )
    {
        List lines = new ArrayList();

        String ind = repeat( "\t", indent );
        String[] plainLines = text.split( "(\r\n)|(\r)|(\n)" );
        for ( int i = 0; i < plainLines.length; i++ )
        {
            toLines( lines, ind + plainLines[i], indentSize, lineLength );
        }

        return lines;
    }

    /** 
     * Adds the specified line to the output sequence, performing line wrapping if necessary.
     * 
     * @param lines The sequence of display lines, must not be <code>null</code>.
     * @param line The line to add, must not be <code>null</code>.
     * @param indentSize The size of each indentation, must not be negative.
     * @param lineLength The length of the line, must not be negative.
     */
    private static void toLines( List lines, String line, int indentSize, int lineLength )
    {
        int lineIndent = getIndentLevel( line );
        StringBuffer buf = new StringBuffer( 256 );
        String[] tokens = line.split( " +" );
        for ( int i = 0; i < tokens.length; i++ )
        {
            String token = tokens[i];
            if ( i > 0 )
            {
                if ( buf.length() + token.length() >= lineLength )
                {
                    lines.add( buf.toString() );
                    buf.setLength( 0 );
                    buf.append( repeat( " ", lineIndent * indentSize ) );
                }
                else
                {
                    buf.append( ' ' );
                }
            }
            for ( int j = 0; j < token.length(); j++ )
            {
                char c = token.charAt( j );
                if ( c == '\t' )
                {
                    buf.append( repeat( " ", indentSize - buf.length() % indentSize ) );
                }
                else if ( c == '\u00A0' )
                {
                    buf.append( ' ' );
                }
                else
                {
                    buf.append( c );
                }
            }
        }
        lines.add( buf.toString() );
    }

    /** 
     * Gets the indentation level of the specified line.
     * 
     * @param line The line whose indentation level should be retrieved, must not be <code>null</code>.
     * @return The indentation level of the line.
     */
    private static int getIndentLevel( String line )
    {
        int level = 0;
        for ( int i = 0; i < line.length() && line.charAt( i ) == '\t'; i++ )
        {
            level++;
        }
        for ( int i = level + 1; i <= level + 4 && i < line.length(); i++ )
        {
            if ( line.charAt( i ) == '\t' )
            {
                level++;
                break;
            }
        }
        return level;
    }
}

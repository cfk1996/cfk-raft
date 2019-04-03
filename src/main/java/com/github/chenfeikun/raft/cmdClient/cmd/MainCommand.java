package com.github.chenfeikun.raft.cmdClient.cmd;

import com.beust.jcommander.JCommander;
import com.github.chenfeikun.raft.BootStrap;
import com.github.chenfeikun.raft.NodeConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * @desciption: MainCommand
 * @CreateTime: 2019-04-02
 * @author: chenfeikun
 */
public class MainCommand {

    public static void main(String[] args) {
        Map<String, BaseCommand> commands = new HashMap<>();
        commands.put("append", new AppendCommand());
        commands.put("get", new GetCommand());

        JCommander.Builder builder = JCommander.newBuilder();
        builder.addCommand("server", new NodeConfig());
        for (String cmd : commands.keySet()) {
            builder.addCommand(cmd, commands.get(cmd));
        }
        JCommander jc = builder.build();
        jc.parse(args);

        if (jc.getParsedCommand() == null) {
            jc.usage();
        } else if (jc.getParsedCommand() == "server") {
            String[] subArgs = new String[args.length-1];
            System.arraycopy(args, 1, subArgs, 0, subArgs.length);
            BootStrap.main(subArgs);
        } else {
            BaseCommand command = commands.get(jc.getParsedCommand());
            if (command != null) {
                command.doCommand();
            } else {
                jc.usage();
            }
        }
    }
}

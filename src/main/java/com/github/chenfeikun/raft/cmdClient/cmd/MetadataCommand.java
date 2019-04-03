package com.github.chenfeikun.raft.cmdClient.cmd;

import com.beust.jcommander.Parameter;
import com.github.chenfeikun.raft.cmdClient.Client;
import com.github.chenfeikun.raft.rpc.entity.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: MetadataCommand
 * @CreateTime: 2019-04-02
 * @author: chenfeikun
 */
public class MetadataCommand implements BaseCommand {

    private static final Logger logger = LoggerFactory.getLogger(MetadataCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--serverId", "-s"}, description = "serverId", required = true)
    private String serverId = "";

    @Parameter(names = {"--peers", "-p"}, description = "peer info of this server")
    private String peers = "n0-localhost:20911;n1-localhost:20912;n2-localhost:20913";

    @Override
    public void doCommand() {
        Client client = new Client(group, peers);
        client.startup();
        MetadataResponse response = client.
    }
}
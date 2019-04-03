package com.github.chenfeikun.raft.cmdClient.cmd;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import com.github.chenfeikun.raft.cmdClient.Client;
import com.github.chenfeikun.raft.rpc.entity.AppendEntryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: AppendCommand
 * @CreateTime: 2019-04-02
 * @author: chenfeikun
 */
public class AppendCommand implements BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(AppendCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:10001;n1-localhost:10002;n2-localhost:10003";

    @Parameter(names = {"--data", "-d"}, description = "the data to append")
    private String data = "Hello";

    @Parameter(names = {"--count", "-c"}, description = "append several times")
    private int count = 1;

    @Override
    public void doCommand() {
        Client client = new Client(group, peers);
        client.startup();
        for (int i = 0; i < count; i++) {
            AppendEntryResponse response = client.append(data.getBytes());
            logger.info("Append Result:{}", JSON.toJSONString(response));
        }
        client.shutdown();
    }
}

package com.github.chenfeikun.raft.cmdClient.cmd;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import com.github.chenfeikun.raft.cmdClient.Client;
import com.github.chenfeikun.raft.core.Entry;
import com.github.chenfeikun.raft.rpc.entity.GetEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @desciption: GetCommand
 * @CreateTime: 2019-04-02
 * @author: chenfeikun
 */
public class GetCommand implements BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(GetCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:10001;n1-localhost:10002;n2-localhost:10003";

    @Parameter(names = {"--index", "-i"}, description = "get entry from index")
    private long index = 0;

    @Override
    public void doCommand() {
        Client client = new Client(group, peers);
        client.startup();
        GetEntriesResponse response = client.get(index);
        logger.info("Get Result:{}", JSON.toJSONString(response));
        if (response.getEntries() != null && response.getEntries().size() > 0) {
            for (Entry entry : response.getEntries()) {
                logger.info("Get Result index:{} {}", entry.getIndex(), new String(entry.getBody()));
            }
        }
        client.shutdown();
    }
}

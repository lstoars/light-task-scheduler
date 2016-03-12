package com.lts.core.domain;

import com.lts.core.cluster.NodeType;
import com.lts.admin.request.PaginationReq;

/**
 * @author Robert HG (254963746@qq.com) on 9/5/15.
 */
public class NodeGroupGetRequest extends PaginationReq {

    private NodeType nodeType;

    private String nodeGroup;

    public NodeType getNodeType() {
        return nodeType;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public String getNodeGroup() {
        return nodeGroup;
    }

    public void setNodeGroup(String nodeGroup) {
        this.nodeGroup = nodeGroup;
    }
}

package com.lts.admin.access.face;

import com.lts.admin.request.JvmDataReq;
import com.lts.admin.request.MDataPaginationReq;
import com.lts.monitor.access.domain.JVMMemoryDataPo;
import com.lts.monitor.access.face.JVMMemoryAccess;

import java.util.List;

/**
 * @author Robert HG (254963746@qq.com) on 9/28/15.
 */
public interface BackendJVMMemoryAccess extends JVMMemoryAccess{

    void delete(JvmDataReq request);

    List<JVMMemoryDataPo> queryAvg(MDataPaginationReq request);
}

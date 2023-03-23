package com.wugui.datax.executor.service.jobhandler;


import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Pair;
import cn.hutool.core.util.ObjectUtil;
import com.wugui.datatx.core.biz.model.ReturnT;
import com.wugui.datatx.core.biz.model.TriggerParam;
import com.wugui.datatx.core.handler.IJobHandler;
import com.wugui.datatx.core.handler.annotation.JobHandler;
import com.wugui.datatx.core.log.JobLogger;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;

/**
 * DataX任务终止
 *
 * @author jingwk 2019-12-16
 */

@JobHandler(value = "killJobHandler")
@Component
public class KillJobHandler extends IJobHandler {

    @Override
    public ReturnT<String> execute(TriggerParam tgParam) {
        String processId = tgParam.getProcessId();
        JobLogger.log("------------------prepare to Kill DataX process id: " + processId);
        JobLogger.log("------------------jobTmpFiles size: " + jobTmpFiles.size());
        if (jobTmpFiles.containsKey(processId)) {
            JobLogger.log("------------------get entry from jobTmpFiles :" + jobTmpFiles.size());
            Pair<String, DefaultExecutor> pair = jobTmpFiles.get(processId);
            if(ObjectUtil.isNotNull(pair)){
                //删除文件
                String pathname = pair.getKey();
                if (pathname != null) {
                FileUtil.del(new File(pathname));
                }
                if(ObjectUtil.isNotNull(pair.getValue())){
                    ExecuteWatchdog executeWatchdog = pair.getValue().getWatchdog();
                    executeWatchdog.destroyProcess();
                }
                jobTmpFiles.remove(pathname);
                return IJobHandler.SUCCESS;
            }
        }
        return IJobHandler.SUCCESS;
    }
}

package com.wugui.datax.executor.service.jobhandler;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Pair;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.IdUtil;
import com.wugui.datatx.core.biz.model.HandleProcessCallbackParam;
import com.wugui.datatx.core.biz.model.ReturnT;
import com.wugui.datatx.core.biz.model.TriggerParam;
import com.wugui.datatx.core.handler.IJobHandler;
import com.wugui.datatx.core.handler.annotation.JobHandler;
import com.wugui.datatx.core.log.JobLogger;
import com.wugui.datatx.core.thread.ProcessCallbackThread;
import com.wugui.datax.executor.service.logparse.LogStatistics;
import com.wugui.datax.executor.util.SystemUtils;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.wugui.datax.executor.service.command.BuildCommand.buildDataXExecutorCmd;
import static com.wugui.datax.executor.service.jobhandler.DataXConstant.DEFAULT_JSON;
import static com.wugui.datax.executor.service.logparse.AnalysisStatistics.analysisStatisticsLog;

/**
 * DataX任务运行
 *
 * @author jingwk 2019-11-16
 */

@JobHandler(value = "executorJobHandler")
@Component
public class ExecutorJobHandler extends IJobHandler {

    @Value("${datax.executor.jsonpath}")
    private String jsonPath;

    @Value("${datax.pypath}")
    private String dataXPyPath;

    private static final long TIME_OUT = 1000 * 60 * 15;


    @Override
    public ReturnT<String> execute(TriggerParam trigger) {

        int exitValue = -1;
        String tmpFilePath;
        LogStatistics logStatistics = null;
        //Generate JSON temporary file
        tmpFilePath = generateTemJsonFile(trigger.getJobJson());

        try {
            String id = UUID.fastUUID().toString(true);
            //update datax process id
            HandleProcessCallbackParam prcs = new HandleProcessCallbackParam(trigger.getLogId(), trigger.getLogDateTime(), id);
            ProcessCallbackThread.pushCallBack(prcs);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

            String[] cmdarrayFinal = buildDataXExecutorCmd(trigger, tmpFilePath, dataXPyPath);
            String cmd = Arrays.stream(cmdarrayFinal).collect(Collectors.joining(" "));
            CommandLine commandline = CommandLine.parse(cmd);
            DefaultExecutor executor = this.initExecutor(outputStream, errorStream, TIME_OUT);

            JobLogger.log("------------------DataX process id: " + id);
            jobTmpFiles.put(id, new Pair<>(tmpFilePath, executor));
            //调用命令行
            exitValue = executor.execute(commandline);
            logStatistics = analysisStatisticsLog(this.parse(outputStream));
            analysisStatisticsLog(this.parse(errorStream));
        } catch (Exception e) {
            JobLogger.log(e);
        } finally {
            //  删除临时文件
            if (FileUtil.exist(tmpFilePath)) {
                FileUtil.del(new File(tmpFilePath));
            }
        }
        if (exitValue == 0) {
            return new ReturnT<>(200, logStatistics.toString());
        } else {
            return new ReturnT<>(IJobHandler.FAIL.getCode(), "command exit value(" + exitValue + ") is failed");
        }
    }


    private String generateTemJsonFile(String jobJson) {
        String tmpFilePath;
        String dataXHomePath = SystemUtils.getDataXHomePath();
        if (StringUtils.isNotEmpty(dataXHomePath)) {
            jsonPath = dataXHomePath + DEFAULT_JSON;
        }
        if (!FileUtil.exist(jsonPath)) {
            FileUtil.mkdir(jsonPath);
        }
        tmpFilePath = jsonPath + "jobTmp-" + IdUtil.simpleUUID() + ".conf";
        // 根据json写入到临时本地文件
        try (PrintWriter writer = new PrintWriter(tmpFilePath, "UTF-8")) {
            writer.println(jobJson);
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            JobLogger.log("JSON 临时文件写入异常：" + e.getMessage());
        }
        return tmpFilePath;
    }

    private ByteArrayInputStream parse(OutputStream out) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos = (ByteArrayOutputStream) out;
        ByteArrayInputStream swapStream = new ByteArrayInputStream(baos.toByteArray());
        return swapStream;
    }

    private DefaultExecutor initExecutor(OutputStream out, OutputStream err, Long timeOut) {
        //看门狗，可设置超时
        ExecuteWatchdog watchdog = new ExecuteWatchdog(TIME_OUT);
        DefaultExecutor exec = new DefaultExecutor();
        exec.setExitValues(null);
        PumpStreamHandler streamHandler = new PumpStreamHandler(out, err);
        exec.setStreamHandler(streamHandler);
        exec.setWatchdog(watchdog);
        exec.setExitValues(null);
        return exec;
    }

}

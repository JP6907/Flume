package com.jp.flume.source;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 自定义source可以参考原有的source实现，如ExecSource
 */

/**
 * com.jp.flume.source.TailFileSource
 * 自动采集指定文件的新增内容
 * 自动维护偏移量
 */
public class TailFileSource extends AbstractSource implements EventDrivenSource, Configurable {
    private static final Logger logger = LoggerFactory.getLogger(TailFileSource.class);

    private String filePath;
    private String charset;
    private String posiPath;
    private long interval;
    private ExecutorService executor;
    private FileRunnable fileRunner;
    /**
     * 1.读取配置文件：读取哪个文件，编码格式，偏移量写到哪个文件，多长时间检查文件是否有新内容
     * @param context
     */
    public void configure(Context context) {
        filePath = context.getString("filePath");
        charset = context.getString("charset");
        posiPath = context.getString("posiPath");
        interval = context.getLong("interval");
    }

    /**
     * 2.创建线程定时读取文件的新数据发送给channel，更新偏移量
     */
    @Override
    public synchronized void start() {
        //创建单线程的线程池
        executor = Executors.newSingleThreadExecutor();
        //创建执行具体任务的线程
        fileRunner = new FileRunnable(filePath,posiPath,charset,interval,getChannelProcessor());
        executor.submit(fileRunner);
        super.start();
    }

    /**
     * 关闭线程池
     * 将flag置为false
     */
    @Override
    public synchronized void stop() {
        fileRunner.setFlag(false);
        executor.shutdown();
        //executor.shutdown();可能没有办法成功关闭，因为线程可能正在处理某些请求，需要再重复关闭几次直到成功
        while (!executor.isTerminated()){
            logger.debug("Waiting for filer executor service to stop");
            try {
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for exec executor service "
                        + "to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }
        super.stop();
    }

    private static class FileRunnable implements Runnable{
        private String charset;
        private Long interval;
        private ChannelProcessor channelProcessor;
        private File positionFile;
        private Long offset = 0L; //偏移量
        private RandomAccessFile raf;
        private boolean flag = true;

        public FileRunnable(String filePath, String posiPath, String charset, Long interval, ChannelProcessor channelProcessor){
            this.charset = charset;
            this.interval = interval;
            this.channelProcessor = channelProcessor;

            //读取偏移量，如果有就接着读取，没有就从头开始读取
            //1.判断储存偏移量的文件是否存在，不存在则创建
            positionFile = new File(posiPath);
            if(!positionFile.exists()){
                try{
                    positionFile.createNewFile();
                }catch (IOException e){
                    logger.error("create position file error",e);
                }
            }
            //2.从文件中读取偏移量
            try {
                String offsetString = FileUtils.readFileToString(positionFile);
                //如果以前记录过偏移量
                if(offsetString!=null && !"".equals(offsetString.trim())){
                    offset = Long.parseLong(offsetString);
                }
                raf = new RandomAccessFile(filePath,"r");
                raf.seek(offset);
            } catch (IOException e) {
                logger.error("read position file error",e);
            }
        }

        public void setFlag(boolean flag){
            this.flag = flag;
        }

        public void run() {
            while (flag){
                //尝试读取文件中的数据，如果没有数据就休眠一段时间interval
                try {
                    String line = raf.readLine();
                    if(line!=null){
                        //默认是ISO-8859-1，因为channel不知道会接收怎样的数据，需要固定每个字符大小
                        line = new String(line.getBytes("ISO-8859-1"),charset);
                        //将数据发送给channel
                        channelProcessor.processEvent(EventBuilder.withBody(line.getBytes()));
                        //更新偏移量
                        offset = raf.getFilePointer();
                        FileUtils.writeStringToFile(positionFile,offset+"");
                    }else{
                        Thread.sleep(interval);
                    }
                } catch (IOException e) {
                    logger.error("read log file error",e);
                } catch (InterruptedException e) {
                    //休眠的时候被打断
                    logger.error("read file thread interrupted",e);
                }
            }
        }

    }
}

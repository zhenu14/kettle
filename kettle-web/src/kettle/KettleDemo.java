package kettle;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

public class KettleDemo {

    public static void main(String[]args){
        String [] params = {"table1","table2"};
//        runTrans(params,"C:\\Users\\prayer\\Desktop\\1.ktr");
        runTrans(params,"C:\\ideaCode\\kettle-web\\etl\\update_insert_Trans.ktr");
//        runJob(null,null,"C:\\Users\\prayer\\Desktop\\1.kjb");
    }

    /**
     * 调用 kettle trans
     *
     * @param params
     *            多个参数值
     * @param ktrPath
     *            如： String fName= "D:\\kettle\\aaa.ktr";
     */
    public static void runTrans(String[] params, String ktrPath) {
        try {
            KettleEnvironment.init();
            EnvUtil.environmentInit();
            TransMeta transMeta = new TransMeta(ktrPath);
            Trans trans = new Trans(transMeta);
            trans.execute(params);
            trans.waitUntilFinished();
            if (trans.getErrors() > 0) {
                throw new Exception("Errors during transformation execution!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 调用 kettle job
     *
     * @param paraNames
     *            多个参数名
     * @param paraValues
     *            多个参数值
     * @param jobPath
     *            如： String fName= "D:\\kettle\\aaa.kjb";
     */
    public static void runJob(String[] paraNames, String[] paraValues, String jobPath) {
        try {
            KettleEnvironment.init();
            JobMeta jobMeta = new JobMeta(jobPath, null);
            Job job = new Job(null, jobMeta);
            // 向Job 脚本传递参数，脚本中获取参数值：${参数名}
            if (paraNames != null && paraValues != null) {
                for (int i = 0; i < paraNames.length && i < paraValues.length; i++) {
                    job.setVariable(paraNames[i], paraValues[i]);
                }
            }
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0) {
                throw new Exception("Errors during job execution!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

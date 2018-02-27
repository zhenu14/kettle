package kettle;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.StepLogTable;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mergerows.MergeRowsMeta;
import org.pentaho.di.trans.steps.synchronizeaftermerge.SynchronizeAfterMergeMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

public class TransDemo2 {
    /**
     * 两个库中的表名
     */
    public static String bjdt_tablename = "table1";
    public static String kettle_tablename = "table2";
    public static String kettle_log = "log";

    /**
     * 数据库连接信息,适用于DatabaseMeta其中 一个构造器DatabaseMeta(String xml)
     */
    public static final String[] databasesXML = {
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<connection>" +
                    "<name>demo</name>" +
                    "<server>127.0.0.1</server>" +
                    "<type>MYSQL</type>" +
                    "<access>Native</access>" +
                    "<database>test</database>" +
                    "<port>3306</port>" +
                    "<username>root</username>" +
                    "<password>56215487</password>" +
                    "</connection>",
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<connection>" +
                    "<name>kettle</name>" +
                    "<server>127.0.0.1</server>" +
                    "<type>MYSQL</type>" +
                    "<access>Native</access>" +
                    "<database>test</database>" +
                    "<port>3306</port>" +
                    "<username>root</username>" +
                    "<password>56215487</password>" +
                    "</connection>"
    };
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            KettleEnvironment.init();
//            transDemo = new KettleDeleteTest();
            System.out.println("************start to generate my own transformation***********");

            TransMeta transMeta = new TransMeta();
            //设置转化的名称
            transMeta.setName("转换名称");

            //添加转换的数据库连接
            for (int i=0;i<databasesXML.length;i++){
                DatabaseMeta databaseMeta = new DatabaseMeta(databasesXML[i]);
                transMeta.addDatabase(databaseMeta);
            }
            VariableSpace space = new Variables();
            //将step日志数据库配置名加入到变量集中
            space.setVariable("kettle_log","demo");
            space.initializeVariablesFrom(null);
            StepLogTable stepLogTable = StepLogTable.getDefault(space,transMeta);
            //StepLogTable使用的数据库连接名（上面配置的变量名）。
            stepLogTable.setConnectionName("demo");
            //设置Step日志的表名
            stepLogTable.setTableName(kettle_log);
            //设置TransMeta的StepLogTable
            transMeta.setStepLogTable(stepLogTable);

            //******************************************************************
            //第一个表输入步骤(原表数据输入)
            TableInputMeta oldTableInput = new TableInputMeta();
            DatabaseMeta database_bjdt = transMeta.findDatabase("test");
            oldTableInput.setDatabaseMeta(database_bjdt);
            String old_select_sql = "SELECT id,param1,param2 FROM "+bjdt_tablename;
            oldTableInput.setSQL(old_select_sql);

            //添加TableInputMeta到转换中
            StepMeta oldTableInputMetaStep = new StepMeta("INPUTTABLE_"+bjdt_tablename,oldTableInput);
            //给步骤添加在spoon工具中的显示位置
            transMeta.addStep(oldTableInputMetaStep);
            //*****************************************************************
            //第二个表输入步骤(原表数据输入)
            TableInputMeta newTableInput = new TableInputMeta();
            //给表输入添加一个DatabaseMeta连接数据库
            DatabaseMeta database_kettle = transMeta.findDatabase("test");
            newTableInput.setDatabaseMeta(database_kettle);
            String new_select_sql = "SELECT id,param1,param2 FROM "+kettle_tablename;
            newTableInput.setSQL(new_select_sql);

            //添加TableInputMeta到转换中
            StepMeta newTableInputMetaStep = new StepMeta("INPUTTABLE_"+kettle_tablename,newTableInput);
            //给步骤添加在spoon工具中的显示位置
            transMeta.addStep(newTableInputMetaStep);
            //******************************************************************

            //******************************************************************
            //第三个步骤合并
            MergeRowsMeta mergeRowsMeta = new MergeRowsMeta();
//设置合并步骤的新旧数据源
            StepIOMetaInterface stepIOMeta = mergeRowsMeta.getStepIOMeta();
            stepIOMeta.getInfoStreams().get(0).setStepMeta(newTableInputMetaStep);
            stepIOMeta.getInfoStreams().get(1).setStepMeta(oldTableInputMetaStep);
            mergeRowsMeta.setFlagField("bz"); //设置标志字段
            mergeRowsMeta.setKeyFields(new String[]{"id"});
            mergeRowsMeta.setValueFields(new String[]{"param1","param2"});
            StepMeta mergeStepMeta = new StepMeta("合并记录", mergeRowsMeta);
            transMeta.addStep(mergeStepMeta);
            //******************************************************************

            //******************************************************************
            //添加HOP把两个输入和合并的步骤关联
            transMeta.addTransHop(new TransHopMeta(oldTableInputMetaStep, mergeStepMeta));
            transMeta.addTransHop(new TransHopMeta(newTableInputMetaStep, mergeStepMeta));
            //******************************************************************

            //******************************************************************
            //第四个步骤同步数据
            SynchronizeAfterMergeMeta synchronizeAfterMergeMeta = new SynchronizeAfterMergeMeta();
            synchronizeAfterMergeMeta.setCommitSize(10000); //设置事务提交数量
            synchronizeAfterMergeMeta.setDatabaseMeta(database_kettle); //目标数据源
            synchronizeAfterMergeMeta.setSchemaName("");//数据表schema
            synchronizeAfterMergeMeta.setTableName(kettle_tablename); //数据表名称
            synchronizeAfterMergeMeta.setUseBatchUpdate(true); //设置批量更新
            //设置用来查询的关键字
            synchronizeAfterMergeMeta.setKeyLookup(new String[]{"id"}); //设置用来查询的关键字
            synchronizeAfterMergeMeta.setKeyStream(new String[]{"id"}); //设置流输入的字段
            synchronizeAfterMergeMeta.setKeyStream2(new String[]{""});//一定要加上
            synchronizeAfterMergeMeta.setKeyCondition(new String[]{"="}); //设置操作符
            //设置要更新的字段
            String[] updatelookup = {"id","param1","param2"} ;
            String [] updateStream = {"id","param1","param2"};
            Boolean[] updateOrNot = {false,true,true};
            synchronizeAfterMergeMeta.setUpdateLookup(updatelookup);
            synchronizeAfterMergeMeta.setUpdateStream(updateStream);
            synchronizeAfterMergeMeta.setUpdate(updateOrNot);

            //设置高级属性(操作)
            synchronizeAfterMergeMeta.setOperationOrderField("bz"); //设置操作标志字段名
            synchronizeAfterMergeMeta.setOrderInsert("new");
            synchronizeAfterMergeMeta.setOrderUpdate("changed");
            synchronizeAfterMergeMeta.setOrderDelete("deleted");
            StepMeta synStepMeta = new StepMeta("数据同步", synchronizeAfterMergeMeta);
            transMeta.addStep(synStepMeta);
            //******************************************************************

            //******************************************************************
            //添加HOP把合并和数据同步的步骤关联
            transMeta.addTransHop(new TransHopMeta(mergeStepMeta,synStepMeta));
            //******************************************************************

            String transXml = transMeta.getXML();
            System.out.println("transXml:"+transXml);

            Trans trans = new Trans(transMeta);

            trans.execute(null); // You can pass arguments instead of null.
            trans.waitUntilFinished();
            if ( trans.getErrors() > 0 )
            {
                throw new RuntimeException( "There were errors during transformation execution." );
            }
            System.out.println("***********the end************");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

    }
}

package com.taos.sentinel.extension.filepull;

import com.alibaba.csp.sentinel.command.handler.ModifyParamFlowRulesCommandHandler;
import com.alibaba.csp.sentinel.datasource.FileRefreshableDataSource;
import com.alibaba.csp.sentinel.datasource.FileWritableDataSource;
import com.alibaba.csp.sentinel.datasource.ReadableDataSource;
import com.alibaba.csp.sentinel.datasource.WritableDataSource;
import com.alibaba.csp.sentinel.init.InitFunc;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRule;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRuleManager;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRuleManager;
import com.alibaba.csp.sentinel.slots.system.SystemRule;
import com.alibaba.csp.sentinel.slots.system.SystemRuleManager;
import com.alibaba.csp.sentinel.transport.util.WritableDataSourceRegistry;

import java.io.FileNotFoundException;
import java.util.List;

/**
 * stay hungry,stay foolish
 * 做个吃货，做个二货！
 *
 * @description: InitFunc 可以作为 sentinel 启动的扩展点，通过源码发现 sentinel 已经提供了 filedatasource 的扩展包，只需要实现datasource 存储和读取功能
 * @author: taosheng
 * @date: 2021/8/18 9:24 下午
 */
public class FileDataSourceInit implements InitFunc {
    @Override
    public void init() throws Exception {
        //创建文件存储目录
        RuleFileUtils.mkdirIfNotExits(PersistenceRuleConstant.storePath);

        //创建规则文件
        RuleFileUtils.createFileIfNotExits(PersistenceRuleConstant.rulesMap);

        //处理流控规则逻辑
        dealFlowRules();
        //降级流控规则处理
        dealDegradeRules();
        // 处理系统规则
        dealSystemRules();
        // 处理热点参数规则
        dealParamFlowRules();
        // 处理授权规则
        dealAuthRules();
    }

    //流控规则处理
    private void dealFlowRules() throws FileNotFoundException {
        //获取流控规则逻辑文件
        String ruleFilePath = PersistenceRuleConstant.rulesMap.get(PersistenceRuleConstant.FLOW_RULE_PATH).toString();

        //创建流控规则的可读数据源
        ReadableDataSource<String, List<FlowRule>> flowRuleRDS = new FileRefreshableDataSource<>(
                ruleFilePath, RuleListConverterUtils.flowRuleListParser
        );

        // 将可读数据源注册至FlowRuleManager 这样当规则文件发生变化时，就会更新规则到内存
        FlowRuleManager.register2Property(flowRuleRDS.getProperty());

        //创建写数据 datasource

        WritableDataSource<List<FlowRule>> flowRuleWDS = new FileWritableDataSource<>(ruleFilePath , RuleListConverterUtils.flowFuleEnCoding);

        // 将可写数据源注册至 transport 模块的 WritableDataSourceRegistry 中.
        // 这样收到控制台推送的规则时，Sentinel 会先更新到内存，然后将规则写入到文件中.
        WritableDataSourceRegistry.registerFlowDataSource(flowRuleWDS);
    }

    //降级规则处理
    private void dealDegradeRules() throws FileNotFoundException {
        //获取降级规则逻辑文件
        String degradeRuleFilePath = PersistenceRuleConstant.rulesMap.get(PersistenceRuleConstant.DEGRAGE_RULE_PATH).toString();

        //创建降级规则的可读数据源
        ReadableDataSource<String, List<DegradeRule>> degradeRuleRDS = new FileRefreshableDataSource<>(
                degradeRuleFilePath, RuleListConverterUtils.degradeRuleListParse
        );

        // 将可读数据源注册至FlowRuleManager 这样当规则文件发生变化时，就会更新规则到内存
        DegradeRuleManager.register2Property(degradeRuleRDS.getProperty());

        //创建写数据源
        WritableDataSource<List<DegradeRule>> writableDataSource = new FileWritableDataSource<>(degradeRuleFilePath, RuleListConverterUtils.degradeRuleEnCoding);

        // 将可写数据源注册至 transport 模块的 WritableDataSourceRegistry 中.
        // 这样收到控制台推送的规则时，Sentinel 会先更新到内存，然后将规则写入到文件中.
        WritableDataSourceRegistry.registerDegradeDataSource(writableDataSource);
    }

    //系统规则处理
    private void dealSystemRules() throws FileNotFoundException {
        //获取系统规则逻辑文件
        String systemRuleFilePath = PersistenceRuleConstant.rulesMap.get(PersistenceRuleConstant.SYSTEM_RULE_PATH).toString();

        //创建系统规则的可读数据源
        ReadableDataSource<String, List<SystemRule>> sysRuleRDS = new FileRefreshableDataSource<>(
                systemRuleFilePath, RuleListConverterUtils.sysRuleListParse
        );

        // 将可读数据源注册至FlowRuleManager 这样当规则文件发生变化时，就会更新规则到内存
        SystemRuleManager.register2Property(sysRuleRDS.getProperty());

        //创建写数据源
        WritableDataSource<List<SystemRule>> writableDataSource = new FileWritableDataSource<>(systemRuleFilePath, RuleListConverterUtils.sysRuleEnCoding);

        // 将可写数据源注册至 transport 模块的 WritableDataSourceRegistry 中.
        // 这样收到控制台推送的规则时，Sentinel 会先更新到内存，然后将规则写入到文件中.
        WritableDataSourceRegistry.registerSystemDataSource(writableDataSource);
    }

    //处理热点参数规则
    private void dealParamFlowRules() throws FileNotFoundException {
        //获取热点参数规则逻辑文件
        String hotParamRuleFilepath = PersistenceRuleConstant.rulesMap.get(PersistenceRuleConstant.HOT_PARAM_RULE).toString();

        //创建热点参数规则的可读数据源
        ReadableDataSource<String, List<ParamFlowRule>> hotParamRuleRDS = new FileRefreshableDataSource<>(
                hotParamRuleFilepath, RuleListConverterUtils.paramFlowRuleListParse
        );

        // 将可读数据源注册至FlowRuleManager 这样当规则文件发生变化时，就会更新规则到内存
        ParamFlowRuleManager.register2Property(hotParamRuleRDS.getProperty());

        //创建写数据源
        WritableDataSource<List<ParamFlowRule>> writableDataSource = new FileWritableDataSource<>(hotParamRuleFilepath, RuleListConverterUtils.paramRuleEnCoding);

        //热点参数特殊处理
        ModifyParamFlowRulesCommandHandler.setWritableDataSource(writableDataSource);
    }

    //授权规则处理
    private void dealAuthRules() throws FileNotFoundException {
        //获取授权规则逻辑文件
        String authFilePath = PersistenceRuleConstant.rulesMap.get(PersistenceRuleConstant.AUTH_RULE_PATH).toString();

        //创建授权规则的可读数据源
        ReadableDataSource<String, List<AuthorityRule>> authorityRuleRDS = new FileRefreshableDataSource<>(
                authFilePath, RuleListConverterUtils.authorityRuleParse
        );

        // 将可读数据源注册至FlowRuleManager 这样当规则文件发生变化时，就会更新规则到内存
        AuthorityRuleManager.register2Property(authorityRuleRDS.getProperty());

        //创建写数据源
        WritableDataSource<List<AuthorityRule>> writableDataSource = new FileWritableDataSource<>(authFilePath, RuleListConverterUtils.authorityEncoding);

        // 将可写数据源注册至 transport 模块的 WritableDataSourceRegistry 中.
        // 这样收到控制台推送的规则时，Sentinel 会先更新到内存，然后将规则写入到文件中.
        WritableDataSourceRegistry.registerAuthorityDataSource(writableDataSource);
    }
}

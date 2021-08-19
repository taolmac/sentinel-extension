package com.taos.sentinel.extension.filepull;

import com.alibaba.csp.sentinel.datasource.Converter;
import com.alibaba.csp.sentinel.slots.block.authority.AuthorityRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.alibaba.csp.sentinel.slots.system.SystemRule;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.List;

/**
 * stay hungry,stay foolish
 * 做个吃货，做个二货！
 *
 * @description: 规则列表解析工具类
 * @author: taosheng
 * @date: 2021/8/19 9:07 上午
 */
public class RuleListConverterUtils {
    //限流规则解析器和序列化器
    public static final Converter<String, List<FlowRule>> flowRuleListParser = new Converter<String, List<FlowRule>>() {
        @Override
        public List<FlowRule> convert(String source) {
            return JSON.parseObject(source, new TypeReference<List<FlowRule>>() {});
        }
    };
    public static final Converter<List<FlowRule>,String> flowFuleEnCoding= new Converter<List<FlowRule>,String>() {

        @Override
        public String convert(List<FlowRule> source) {
            return JSON.toJSONString(source);
        }
    };

    //降级规则解析器和序列化器
    public static final Converter<String,List<DegradeRule>> degradeRuleListParse = new Converter<String, List<DegradeRule>>() {
        @Override
        public List<DegradeRule> convert(String source) {
            return JSON.parseObject(source,new TypeReference<List<DegradeRule>>(){});
        }
    };

    public static final Converter<List<DegradeRule>,String> degradeRuleEnCoding= new Converter<List<DegradeRule>,String>() {

        @Override
        public String convert(List<DegradeRule> source) {
            return JSON.toJSONString(source);
        }
    };

    //系统规则解析器和序列化器
    public static final Converter<String,List<SystemRule>> sysRuleListParse = new Converter<String, List<SystemRule>>() {
        @Override
        public List<SystemRule> convert(String source) {
            return JSON.parseObject(source,new TypeReference<List<SystemRule>>(){});
        }
    };
    public static final Converter<List<SystemRule>,String> sysRuleEnCoding= new Converter<List<SystemRule>,String>() {
        @Override
        public String convert(List<SystemRule> source) {
            return JSON.toJSONString(source);
        }
    };

    //参数限流规则解析器和序列化器
    public static final Converter<String,List<ParamFlowRule>> paramFlowRuleListParse = new Converter<String, List<ParamFlowRule>>() {
        @Override
        public List<ParamFlowRule> convert(String source) {
            return JSON.parseObject(source,new TypeReference<List<ParamFlowRule>>(){});
        }
    };
    public static final Converter<List<ParamFlowRule>,String> paramRuleEnCoding= new Converter<List<ParamFlowRule>,String>() {

        @Override
        public String convert(List<ParamFlowRule> source) {
            return JSON.toJSONString(source);
        }
    };

    //权限规则解析器和序列化器
    public static final Converter<String,List<AuthorityRule>> authorityRuleParse = new Converter<String, List<AuthorityRule>>() {
        @Override
        public List<AuthorityRule> convert(String source) {
            return JSON.parseObject(source,new TypeReference<List<AuthorityRule>>(){});
        }
    };
    public static final Converter<List<AuthorityRule>,String> authorityEncoding= new Converter<List<AuthorityRule>,String>() {

        @Override
        public String convert(List<AuthorityRule> source) {
            return JSON.toJSONString(source);
        }
    };
}

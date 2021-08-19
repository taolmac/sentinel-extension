package com.taos.sentinel.extension.filepull;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * stay hungry,stay foolish
 * 做个吃货，做个二货！
 *
 * @description: 创建规则持久化目录和文件的工具类
 * @author: taosheng
 * @date: 2021/8/18 9:35 下午
 */
public class RuleFileUtils {

    public static void mkdirIfNotExits(String filePath) throws IOException {
        File file = new File(filePath);
        if(!file.exists()) {
            file.mkdirs();
        }
    }

    public static void createFileIfNotExits(Map<String,String> ruleFileMap) throws IOException {

        Set<String> ruleFilePathSet = ruleFileMap.keySet();
        Iterator<String> ruleFilePathIter = ruleFilePathSet.iterator();
        while (ruleFilePathIter.hasNext()) {
            String ruleFilePathKey = ruleFilePathIter.next();
            String ruleFilePath  = PersistenceRuleConstant.rulesMap.get(ruleFilePathKey).toString();
            File ruleFile = new File(ruleFilePath);
            if(!ruleFile.exists()) {
                ruleFile.createNewFile();
            }
        }
    }

}

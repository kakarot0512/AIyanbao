# 慧博研报爬虫与AI分析系统

这个项目包含两个主要功能：
1. 爬取慧博研报网站的研报数据
2. 使用Google Gemini AI模型分析研报数据并通过方糖推送摘要

## 功能介绍

### 慧博研报爬虫
- 爬取投研资讯网的研报数据
- 爬取最新买入研报数据
- 爬取热门研报数据（今日热门、本周热门、本月热门）
- 爬取行业分析和投资策略研报数据
- 数据去重并保存为CSV文件

### 研报AI分析
- 使用Google Gemini模型分析研报数据
- 识别核心投资主题
- 构建高成长潜力模拟投资组合
- 通过方糖推送分析结果

## 环境变量配置

在GitHub Actions中需要配置以下密钥：

1. `GEMINI_API_KEY`: Google Gemini API密钥
   - 获取方法：访问 https://ai.google.dev/ 注册并创建API密钥


2. `FANGTANG_KEY`: 方糖推送密钥
   - 获取方法：访问 https://sct.ftqq.com/ 注册并获取推送密钥


## 本地运行

### 安装依赖
```bash
pip install pandas beautifulsoup4 undetected-chromedriver selenium google-generativeai requests
```

### 设置环境变量
```bash
# Linux/Mac
export GEMINI_API_KEY="你的Gemini API密钥"
export FANGTANG_KEY="你的方糖推送密钥"

# Windows
set GEMINI_API_KEY=你的Gemini API密钥
set FANGTANG_KEY=你的方糖推送密钥
```

### 爬取研报数据
```bash
python 慧博研报爬虫整合版.py
```

### 运行AI分析并推送
```bash
python appstore_monitor.py
```

## 自动化运行

本项目使用GitHub Actions自动化运行，每小时执行一次：
1. 爬取最新研报数据
2. 使用AI分析研报并推送结果
3. 将更新的数据提交回仓库

## 注意事项

1. 确保在GitHub仓库的Settings -> Secrets and variables -> Actions中配置了`GEMINI_API_KEY`和`FANGTANG_KEY`
2. 研报数据保存在`研报数据`目录下
3. 最新的研报数据保存为`慧博研报_最新数据.csv`

## 错误排查

1. 方糖推送失败
   - 检查`FANGTANG_KEY`是否正确
   - 检查方糖服务是否可用
   - 查看日志中是否有详细错误信息

2. Gemini API调用失败
   - 检查`GEMINI_API_KEY`是否正确
   - 检查API密钥是否有效
   - 查看日志中是否有详细错误信息

3. 爬虫失败
   - 检查网站结构是否发生变化
   - 检查网络连接是否正常
   - 尝试更新Chrome浏览器版本

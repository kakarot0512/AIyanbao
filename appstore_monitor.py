#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import logging
import os
import sys
import pandas as pd
import json
import time
import pytz
from datetime import datetime, timezone, timedelta
import google.generativeai as genai

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 配置参数
FANGTANG_KEY = os.environ.get("FANGTANG_KEY", "")  # 从环境变量获取方糖 KEY
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")  # 从环境变量获取 Gemini API KEY
LATEST_REPORT_FILE = "研报数据/慧博研报_最新数据.csv"  # 最新研报数据文件
FINANCIAL_NEWS_DIR = "财经新闻数据"  # 财经新闻数据目录
CLS_NEWS_DIR = "财联社/output/cls"  # 财联社新闻数据目录

# 创建数据目录
os.makedirs(FINANCIAL_NEWS_DIR, exist_ok=True)

# 配置 Gemini API
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    
# 综合分析师角色描述
ANALYST_PROMPT = """
# 角色
你是一位拥有15年经验的中国A股基金经理和首席投资策略分析师，尤其擅长从海量、混杂的券商研报和市场信息中，通过交叉验证和逻辑推演，挖掘出具备"预期差"和高成长潜力的投资机会。

# 背景
你获得了三类数据作为信息源：
1. 近期发布的一批A股券商研究报告
2. 最新财经新闻汇总
3. 财联社电报实时资讯

你知道这些信息可能存在滞后性、片面性甚至错误，因此你的核心价值在于独立思考和深度甄别，而非简单复述。

# 任务
请你基于下面提供的参考资料，严格遵循以下分析框架，为我构建并详细阐述一个由8-12只A股组成的【高成长潜力模拟投资组合】。

**分析框架 (请严格按步骤执行):**

1.  **宏观主题识别 (Theme Identification):**
    * 快速扫描所有研报摘要和财经新闻，识别并归纳出当前市场关注度最高、被多家券商反复提及或新闻热点的2-4个核心投资主题或赛道（例如：AI硬件、出海龙头、机器人产业链、半导体国产化、消费电子复苏等）。

2.  **多源交叉验证 (Cross-Validation):**
    * 在识别出的每个核心主题下，筛选出被 **至少2家或以上不同券商** 同时给予"买入"、"增持"或同等正面评级的个股，形成初步候选池。
    * 对比不同研报对同一家公司的核心观点，并结合最新财经新闻，标记出其中的 **共识（Consensus）** 与 **分歧（Divergence）**。共识部分是投资逻辑的基石，分歧部分则可能隐藏着风险或超额收益的机会。

3.  **个股深度剖析 (Deep Dive Analysis):**
    * 从候选池中，基于以下标准挑选最终入选组合的个股：
        * **成长驱动力清晰**: 公司的主营业务增长逻辑是否强劲且可持续？（例如：技术突破、新订单、产能扩张、市占率提升）。
        * **业绩可见性高**: 研报或新闻中是否提及具体的业绩预告、订单合同、或明确的业绩改善信号？
        * **估值相对合理**: 虽然是成长组合，但其估值是否在同业或历史中具有相对吸引力？(可基于研报摘要信息做初步判断)

4.  **投资组合构建与风险管理 (Portfolio Construction & Risk Management):**
    * 最终构建一个包含8-12只股票的投资组合。
    * 组合内应适当分散，覆盖你识别出的主要核心主题，避免在单一赛道上过度集中。
    * 为每只入选的股票，明确其在组合中的定位（例如："核心配置"代表逻辑最强、确定性高；"卫星配置"代表弹性较大、属于博取更高收益的部分）。

**输出格式 (请严格按照以下结构呈现):**

**一、 市场核心洞察与投资策略**
* （简要总结你从研报和财经新闻中感知到的整体市场情绪、热点板块轮动特征，以及你本次构建组合的核心策略。）

**二、 精选核心投资主题**
* **主题一：** [例如：AI与机器人]
* **主题二：** [例如：全球化与出海企业]
* **主题三：** [例如：半导体与高端制造]

**三、 高成长潜力模拟投资组合详情**
（请使用表格呈现）
| 股票代码 | 公司名称 | 核心投资逻辑 (一句话概括) | 成长驱动因素 | 主要风险提示 | 券商共识评级 | 组合内定位 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
|          |          |                               |              |              |              |            |
|          |          |                               |              |              |              |            |
|   ...    |   ...    |                               |      ...       |     ...      |      ...       |    ...     |

# 参考资料

## 1. 研报数据
{reports_data}

## 2. 财经新闻汇总
{financial_news}

## 3. 财联社电报
{cls_news}
"""

def get_china_time():
    """获取中国时间"""
    # 获取当前 UTC 时间
    utc_now = datetime.now(timezone.utc)
    # 转换为中国时间 (UTC+8)
    china_now = utc_now + timedelta(hours=8)
    return china_now

def send_to_fangtang(title, content, short):
    """发送消息到方糖"""
    if not FANGTANG_KEY:
        logging.warning("未设置方糖 KEY，跳过推送")
        return False
    
    try:
        url = f"https://sctapi.ftqq.com/{FANGTANG_KEY}.send"
        data = {
            "title": title,
            "desp": content,
            "short": short
        }
        response = requests.post(url, data=data, timeout=10)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") == 0:
            logging.info("方糖推送成功")
            return True
        else:
            logging.error(f"方糖推送失败: {result.get('message', '未知错误')}")
            return False
    
    except Exception as e:
        logging.error(f"方糖推送异常: {str(e)}")
        return False

def load_research_reports():
    """加载最新的研报数据"""
    try:
        if os.path.exists(LATEST_REPORT_FILE):
            # 直接读取CSV文件内容，不做处理
            with open(LATEST_REPORT_FILE, 'r', encoding='utf-8-sig') as f:
                csv_content = f.read()
            logging.info(f"成功加载研报数据文件")
            return csv_content
        else:
            logging.warning(f"研报数据文件 {LATEST_REPORT_FILE} 不存在")
            return None
    except Exception as e:
        logging.error(f"加载研报数据失败: {str(e)}")
        return None

def load_financial_news():
    """加载最新的财经新闻数据"""
    try:
        # 获取当前年月
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        
        # 构建当月文件名
        archive_filename = f"financial_news_archive-{current_year}-{current_month:02d}.csv"
        file_path = os.path.join(FINANCIAL_NEWS_DIR, archive_filename)
        
        if os.path.exists(file_path):
            # 读取CSV文件
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            
            # 只取最近200条新闻
            if len(df) > 200:
                df = df.head(200)
            
            # 转换为字符串
            news_content = df.to_string(index=False)
            logging.info(f"成功加载财经新闻数据文件，共 {len(df)} 条记录")
            return news_content
        else:
            logging.warning(f"财经新闻数据文件 {file_path} 不存在")
            return "暂无财经新闻数据"
    except Exception as e:
        logging.error(f"加载财经新闻数据失败: {str(e)}")
        return "加载财经新闻数据失败"

def load_cls_news():
    """加载最新的财联社新闻数据"""
    try:
        # 获取当前周的文件
        current_date = datetime.now()
        week_str = f"{current_date.year}-W{current_date.isocalendar()[1]:02d}"
        
        # 构建文件路径
        file_path = os.path.join(CLS_NEWS_DIR, f"cls_{week_str}.md")
        
        if os.path.exists(file_path):
            # 读取文件内容
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            logging.info(f"成功加载财联社新闻数据文件")
            return content
        else:
            logging.warning(f"财联社新闻数据文件 {file_path} 不存在")
            return "暂无财联社新闻数据"
    except Exception as e:
        logging.error(f"加载财联社新闻数据失败: {str(e)}")
        return "加载财联社新闻数据失败"

def generate_comprehensive_analysis(reports_data, financial_news, cls_news):
    """使用 Gemini 模型生成综合分析报告"""
    if not GEMINI_API_KEY:
        logging.warning("未设置 Gemini API KEY，跳过生成分析")
        return "未配置 Gemini API KEY，无法生成分析"
    
    try:
        # 使用 Gemini 模型
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # 准备提示词
        prompt = ANALYST_PROMPT.format(
            reports_data=reports_data,
            financial_news=financial_news,
            cls_news=cls_news
        )
        
        # 生成分析
        logging.info("开始使用 Gemini 生成综合分析...")
        response = model.generate_content(prompt)
        
        if response and hasattr(response, 'text'):
            logging.info("成功生成综合分析")
            return response.text
        else:
            logging.error("生成分析失败: 响应格式异常")
            return "生成分析失败: 响应格式异常"
    
    except Exception as e:
        logging.error(f"生成分析失败: {str(e)}")
        return f"生成分析失败: {str(e)}"

def send_analysis_report(analysis):
    """发送分析报告到方糖"""
    if not FANGTANG_KEY:
        logging.warning("未设置方糖 KEY，跳过推送")
        return False
    
    try:
        # 获取中国时间并格式化
        china_time = get_china_time()
        time_str = china_time.strftime('%Y-%m-%d %H:%M')
        
        # 构建推送标题和内容
        title = f"投资数据综合AI分析 - {time_str} (中国时间)"
        content = analysis
        short = "投资数据综合AI分析已生成"
        
        # 发送到方糖
        return send_to_fangtang(title, content, short)
    
    except Exception as e:
        logging.error(f"发送分析报告失败: {str(e)}")
        return False

def process_all_data():
    """处理所有数据并发送综合分析"""
    logging.info("开始处理所有数据...")
    
    # 加载研报数据
    reports_data = load_research_reports() or "暂无研报数据"
    
    # 加载财经新闻数据
    financial_news = load_financial_news()
    
    # 加载财联社新闻数据
    cls_news = load_cls_news()
    
    # 生成综合分析
    analysis = generate_comprehensive_analysis(reports_data, financial_news, cls_news)
    
    # 发送分析报告
    if analysis:
        success = send_analysis_report(analysis)
        if success:
            logging.info("综合分析报告已成功推送")
        else:
            logging.error("综合分析报告推送失败")
    else:
        logging.error("未生成综合分析报告，跳过推送")

if __name__ == "__main__":
    # 处理所有数据
    process_all_data()
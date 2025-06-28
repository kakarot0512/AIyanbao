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
你是一位拥有丰富经验的中国A股基金经理和首席（投资、投机）策略分析师，对冲基金量化分析师，尤其擅长从海量、混杂的券商研报、财经新闻和实时资讯中，通过交叉验证和逻辑推演，挖掘出潜在的（投资、投机、套利）机会。

# 背景
当前时间为{current_time}。你正在进行投研的准备工作。你的信息源包括：
1.  近期（过去一周）发布的A股券商研究报告。
2.  最新的宏观经济数据和财经新闻。
3.  财联社电报等实时市场资讯。

你知道这些信息充满了"噪音"，且可能存在滞后性、片面性甚至错误。因此，你的核心价值在于 **快速过滤、精准提炼、独立思考和深度甄别**，而非简单复述。

# 任务
请你基于下面提供的参考资料，严格遵循以下 **"投研晨会"** 的分析框架，为我构建并详细阐述一个由8-12只A股组成的【高成长潜力模拟投资组合】。

**分析框架 (请严格按步骤执行):**

1.  **市场关键信息梳理与定调 (Market Intelligence Briefing & Tone Setting):**
    * **此为首要步骤。** 请首先从所有参考资料（研报、新闻、电报）中，提炼出对今日乃至近期A股投资有 **重大影响** 的关键信息。
    * 将这些信息分类整理为以下三部分，并简要评估其潜在影响（利好[+]、利空[-]、中性或不确定[~]）：
        * **宏观与政策动态:** 如重要的经济数据发布、产业政策、监管动向、国际关系等。
        * **产业与科技前沿:** 如关键技术突破（如固态电池、AI模型）、产业链价格异动、重要行业会议结论等。**特别关注：重要商品期货（如原油、铜、黄金）及工业原料（如锂、稀土）、中美国债利率的价格趋势，并分析其对上下游产业链（如采掘、冶炼、制造、化工）的成本和利润传导效应。**
        * **焦点公司与市场异动:** 如龙头公司的重大合同、业绩预警/预喜、并购重组、以及市场普遍关注的突发新闻。

2.  **核心投资主题识别 (Theme Identification):**
    * 基于 **第一步梳理出的关键信息** 和所有研报摘要，识别并归纳出当前市场关注度最高、逻辑最顺、催化剂最强的2-4个核心投资主题或赛道。
    * 每个主题需用一句话阐明其核心逻辑（例如：AI算力需求爆发，带动上游硬件产业链景气度持续提升）。

3.  **多源交叉验证与个股筛选 (Cross-Validation & Stock Screening):**
    * 在识别出的每个核心主题下，筛选出被 **至少2家或以上不同券商** 同时给予"买入"、"增持"或同等正面评级的个股。
    * 结合第一步整理的最新新闻，对候选公司进行二次验证，剔除存在潜在重大利空信息的公司，形成最终候选池。

4.  **个股深度剖析 (Deep Dive Analysis):**
    * 从候选池中，基于以下标准挑选最终入选组合的个股：
        * **成长驱动力清晰**: 公司的主营业务增长逻辑是否强劲且可持续？
        * **业绩可见性高**: 研报或新闻中是否提及具体的业绩预告、订单合同、或明确的业绩改善信号？
        * **催化剂时效性**: 公司是否与近期热点新闻或政策直接相关，具备短期催化效应？

5.  **投资组合构建与风险管理 (Portfolio Construction & Risk Management):**
    * 最终构建一个包含8-12只股票的投资组合。
    * 组合内应适当分散，覆盖你识别出的主要核心主题，避免在单一赛道上过度集中。
    * 为每只入选的股票，明确其在组合中的定位（例如："核心配置"代表逻辑最强、确定性高；"卫星配置"代表弹性较大、属于博取更高收益的部分）。

**输出格式 (请严格按照以下结构呈现):**

**一、 市场关键信息速览 (Market Intelligence Dashboard)**
* **宏观与政策动态:**
    * （信息点1）[影响评估: +/-/~]
    * （信息点2）[影响评估: +/-/~]
* **产业与科技前沿:**
    * （信息点1）[影响评估: +/-/~]
    * （信息点2）[影响评估: +/-/~]
* **焦点公司与市场异动:**
    * （信息点1）[影响评估: +/-/~]
    * （信息点2）[影响评估: +/-/~]

**二、 市场核心洞察与投资策略**
* （基于第一部分的信息，简要总结你对当前市场情绪的判断、识别出的主要机会与风险，并阐述本次构建组合的核心策略。）

**三、 精选核心投资主题**
* **主题一：** [主题名称] - [核心逻辑阐述]
* **主题二：** [主题名称] - [核心逻辑阐述]
* **主题三：** [主题名称] - [核心逻辑阐述]

**四、 高成长潜力模拟投资组合详情**
（请使用表格呈现）
| 股票代码 | 公司名称 | 核心投资逻辑 (一句话概括) | 成长驱动因素与近期催化剂 | 主要风险提示 | 组合内定位 |
| :--- | :--- | :--- | :--- | :--- | :--- |
|          |          |                               |                          |              |            |
|   ...    |   ...    |            ...                  |           ...                |     ...      |    ...     |

**五、 投资组合风险声明**
* 本模拟投资组合完全基于所提供的历史信息构建，仅作为投资研究和分析思路的展示。所有信息和观点均有时效性，不构成任何实际的投资建议。投资者在做出任何投资决策前，必须进行独立的深入研究和风险评估。市场有风险，投资需谨慎。

# 参考资料
（此部分用于后续粘贴具体的信息源）

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
    """加载当前周和上一周的财联社新闻数据"""
    try:
        # 获取当前周和上一周的信息
        current_date = datetime.now()
        current_week_num = current_date.isocalendar()[1]
        current_year = current_date.year
        
        # 计算上一周的周数和年份
        prev_week_date = current_date - timedelta(days=7)
        prev_week_num = prev_week_date.isocalendar()[1]
        prev_year = prev_week_date.year
        
        # 构建当前周和上一周的文件路径
        current_week_str = f"{current_year}-W{current_week_num:02d}"
        prev_week_str = f"{prev_year}-W{prev_week_num:02d}"
        
        current_file_path = os.path.join(CLS_NEWS_DIR, f"cls_{current_week_str}.md")
        prev_file_path = os.path.join(CLS_NEWS_DIR, f"cls_{prev_week_str}.md")
        
        # 初始化内容变量
        current_week_content = ""
        prev_week_content = ""
        
        # 尝试读取当前周的数据
        if os.path.exists(current_file_path):
            with open(current_file_path, 'r', encoding='utf-8') as f:
                current_week_content = f.read()
            logging.info(f"成功加载当前周 ({current_week_str}) 财联社新闻数据")
        else:
            logging.warning(f"当前周 ({current_week_str}) 财联社新闻数据文件不存在")
        
        # 尝试读取上一周的数据
        if os.path.exists(prev_file_path):
            with open(prev_file_path, 'r', encoding='utf-8') as f:
                prev_week_content = f.read()
            logging.info(f"成功加载上一周 ({prev_week_str}) 财联社新闻数据")
        else:
            logging.warning(f"上一周 ({prev_week_str}) 财联社新闻数据文件不存在")
        
        # 组合两周的数据
        combined_content = ""
        
        if current_week_content:
            combined_content += f"# 当前周 ({current_week_str}) 财联社电报\n\n{current_week_content}\n\n"
            
        if prev_week_content:
            combined_content += f"# 上一周 ({prev_week_str}) 财联社电报\n\n{prev_week_content}"
        
        # 如果两周都没有数据，返回提示信息
        if not combined_content:
            return "暂无财联社新闻数据"
            
        return combined_content
        
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
        
        # 获取当前时间格式化字符串
        current_time = datetime.now().strftime('%Y年%m月%d日 %H:%M')
        
        # 准备提示词
        prompt = ANALYST_PROMPT.format(
            reports_data=reports_data,
            financial_news=financial_news,
            cls_news=cls_news,
            current_time=current_time
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

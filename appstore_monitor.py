#!/usr/bin/env python3
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
from google.generativeai import types

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 配置参数
FANGTANG_KEY = os.environ.get("FANGTANG_KEY", "")  # 从环境变量获取方糖 KEY
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")  # 从环境变量获取 Gemini API KEY
LATEST_REPORT_FILE = "研报数据/慧博研报_最新数据.csv"  # 最新研报数据文件
FINANCIAL_NEWS_DIR = "财经新闻数据"  # 财经新闻数据目录
CLS_NEWS_DIR = "财联社/output/cls"  # 财联社新闻数据目录
MARKET_DATA_DIR = "国际市场数据"  # 国际市场数据目录
DAILY_REPORT_DIR = "每日报告" # 每日报告输出目录

# 创建数据目录
logging.info("开始创建所需的数据目录...")
os.makedirs(FINANCIAL_NEWS_DIR, exist_ok=True)
logging.info(f"目录 '{FINANCIAL_NEWS_DIR}' 已确认存在。")
os.makedirs(MARKET_DATA_DIR, exist_ok=True)
logging.info(f"目录 '{MARKET_DATA_DIR}' 已确认存在。")
os.makedirs(DAILY_REPORT_DIR, exist_ok=True)
logging.info(f"目录 '{DAILY_REPORT_DIR}' 已确认存在。")
logging.info("数据目录创建完成。")


# 配置 Gemini API
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    logging.info("Gemini API KEY 已配置。")
else:
    logging.warning("警告: 未找到 Gemini API KEY 环境变量。")

# 综合分析师角色描述
ANALYST_PROMPT = """
# 角色
你是一位拥有丰富经验的中国A股基金经理和首席（投资、投机）策略分析师，对冲基金量化分析师，尤其擅长从海量、混杂的券商研报、财经新闻和实时资讯中，通过交叉验证和逻辑推演，挖掘出潜在的（投资、投机、套利）机会。

# 背景
当前时间为{current_time}。你正在进行投研的准备工作。你的信息源包括：
1.  近期（过去一周）发布的A股券商研究报告。
2.  最新的宏观经济数据和财经新闻。
3.  财联社电报等实时市场资讯。
4.  全球主要股指、大宗商品、外汇和利率等国际市场数据，包括黄金、原油、铜等大宗商品价格走势，美元指数，中美国债收益率，以及A股主要指数表现。

你知道这些信息充满了"噪音"，且可能存在滞后性、片面性甚至错误。因此，你的核心价值在于 **快速过滤、精准提炼、独立思考和深度甄别**，而非简单复述。

# 任务
请你基于下面提供的参考资料，严格遵循以下投资的分析框架，先判断中国a股是否有重大下行风险。在没有重大下行风险的情况下，为我构建并详细阐述一个由8-12只A股或者ETF组成的近期可以赚钱投资组合。

**分析框架 (请严格按步骤执行):**

1.  **市场关键信息梳理与定调 (Market Intelligence Briefing & Tone Setting):**
    * **此为首要步骤。** 请首先从所有参考资料（研报、新闻、电报、国际市场数据）中，提炼出对今日乃至近期A股投资有 **重大影响** 的关键信息。
    * 将这些信息分类整理为以下三部分，并简要评估其潜在影响（利好[+]、利空[-]、中性或不确定[~]）：
        * **宏观与政策动态:** 如重要的经济数据发布、产业政策、监管动向、国际关系等。特别关注国际市场分析中提供的全球市场情绪指标，如VIX指数、美元指数等。
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
|          |          |                           |                          |              |            |
|   ...    |   ...    |            ...              |            ...             |    ...       |    ...     |

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

## 4. 国际市场分析
{market_analysis}
"""

def get_china_time():
    """获取中国时间"""
    utc_now = datetime.now(timezone.utc)
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
            with open(LATEST_REPORT_FILE, 'r', encoding='utf-8-sig') as f:
                csv_content = f.read()
            logging.info(f"成功加载研报数据文件: {LATEST_REPORT_FILE}")
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
        current_date = get_china_time()
        current_year = current_date.year
        current_month = current_date.month
        
        archive_filename = f"financial_news_archive-{current_year}-{current_month:02d}.csv"
        file_path = os.path.join(FINANCIAL_NEWS_DIR, archive_filename)
        
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            
            if len(df) > 200:
                df = df.head(200)
            
            news_content = df.to_string(index=False)
            logging.info(f"成功加载财经新闻数据文件: {file_path}，共 {len(df)} 条记录")
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
        current_date = get_china_time()
        current_week_num = current_date.isocalendar()[1]
        current_year = current_date.year
        
        prev_week_date = current_date - timedelta(days=7)
        prev_week_num = prev_week_date.isocalendar()[1]
        prev_year = prev_week_date.year
        
        current_week_str = f"{current_year}-W{current_week_num:02d}"
        prev_week_str = f"{prev_year}-W{prev_week_num:02d}"
        
        current_file_path = os.path.join(CLS_NEWS_DIR, f"cls_{current_week_str}.md")
        prev_file_path = os.path.join(CLS_NEWS_DIR, f"cls_{prev_week_str}.md")
        
        current_week_content = ""
        prev_week_content = ""
        
        if os.path.exists(current_file_path):
            with open(current_file_path, 'r', encoding='utf-8') as f:
                current_week_content = f.read()
            logging.info(f"成功加载当前周 ({current_week_str}) 财联社新闻数据")
        else:
            logging.warning(f"当前周 ({current_week_str}) 财联社新闻数据文件不存在")
        
        if os.path.exists(prev_file_path):
            with open(prev_file_path, 'r', encoding='utf-8') as f:
                prev_week_content = f.read()
            logging.info(f"成功加载上一周 ({prev_week_str}) 财联社新闻数据")
        else:
            logging.warning(f"上一周 ({prev_week_str}) 财联社新闻数据文件不存在")
        
        combined_content = ""
        
        if current_week_content:
            combined_content += f"# 当前周 ({current_week_str}) 财联社电报\n\n{current_week_content}\n\n"
            
        if prev_week_content:
            combined_content += f"# 上一周 ({prev_week_str}) 财联社电报\n\n{prev_week_content}"
        
        if not combined_content:
            return "暂无财联社新闻数据"
            
        return combined_content
        
    except Exception as e:
        logging.error(f"加载财联社新闻数据失败: {str(e)}")
        return "加载财联社新闻数据失败"

def load_market_data():
    """加载最新的国际市场数据"""
    try:
        market_data_file = os.path.join(MARKET_DATA_DIR, "global_market_data_latest.json")
        market_analysis_file = os.path.join(MARKET_DATA_DIR, "market_analysis_" + get_china_time().strftime('%Y-%m-%d') + ".md")
        
        market_data = None
        market_analysis = None
        
        if os.path.exists(market_data_file):
            with open(market_data_file, 'r', encoding='utf-8') as f:
                market_data = json.load(f)
            logging.info(f"成功加载国际市场数据: {market_data_file}")
        else:
            logging.warning(f"国际市场数据文件 {market_data_file} 不存在")
        
        if os.path.exists(market_analysis_file):
            with open(market_analysis_file, 'r', encoding='utf-8') as f:
                market_analysis = f.read()
            logging.info(f"成功加载国际市场分析: {market_analysis_file}")
        else:
            analysis_files = [f for f in os.listdir(MARKET_DATA_DIR) if f.startswith("market_analysis_") and f.endswith(".md")]
            if analysis_files:
                latest_file = sorted(analysis_files)[-1]
                with open(os.path.join(MARKET_DATA_DIR, latest_file), 'r', encoding='utf-8') as f:
                    market_analysis = f.read()
                logging.info(f"成功加载最近的国际市场分析: {latest_file}")
            else:
                logging.warning("未找到任何国际市场分析文件")
        
        return market_data, market_analysis
    except Exception as e:
        logging.error(f"加载国际市场数据失败: {e}")
        return None, None

def generate_comprehensive_analysis(reports_data, financial_news, cls_news, market_analysis):
    """使用 Gemini 模型生成综合分析报告"""
    if not GEMINI_API_KEY:
        logging.warning("未设置 Gemini API KEY，跳过生成分析")
        return "未配置 Gemini API KEY，无法生成分析"
    
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        china_time = get_china_time()
        weekday_names = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
        weekday = weekday_names[china_time.weekday()]
        current_time = f"{china_time.strftime('%Y年%m月%d日 %H:%M')} {weekday}"
        
        prompt = ANALYST_PROMPT.format(
            reports_data=reports_data,
            financial_news=financial_news,
            cls_news=cls_news,
            current_time=current_time,
            market_analysis=market_analysis
        )
        
        logging.info("开始使用 Gemini 生成综合分析...")
        response = model.generate_content(prompt)
        
        analysis_text = ""
        if response and hasattr(response, 'text'):
            analysis_text = response.text
        elif response and response.parts:
            try:
                analysis_text = response.parts[0].text
            except (IndexError, AttributeError) as e:
                 logging.error(f"无法从 response.parts 提取文本: {e}")
                 analysis_text = "生成分析失败: 响应格式异常"
        else:
            logging.error("生成分析失败: 响应格式异常或内容为空")
            analysis_text = "生成分析失败: 响应格式异常"

        logging.info(f"成功生成综合分析，内容长度: {len(analysis_text)} 字符。")
        return analysis_text

    except Exception as e:
        logging.error(f"生成分析时发生异常: {str(e)}")
        return f"生成分析失败: {str(e)}"

def save_analysis_to_file(analysis_content):
    """将分析报告保存到指定目录的 MD 文件中"""
    logging.info("开始执行保存分析报告到文件的函数...")
    try:
        china_time = get_china_time()
        date_str = china_time.strftime('%Y-%m-%d')
        time_str = china_time.strftime('%H%M%S')

        report_date_dir = os.path.join(DAILY_REPORT_DIR, date_str)
        logging.info(f"准备创建日期子目录: {report_date_dir}")
        os.makedirs(report_date_dir, exist_ok=True)
        logging.info("日期子目录确认完毕。")

        file_name = f"analysis_report_{time_str}.md"
        file_path = os.path.join(report_date_dir, file_name)
        logging.info(f"最终文件路径为: {file_path}")

        logging.info("开始写入文件...")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(analysis_content)
        logging.info(f"文件写入成功: {file_path}")

        return True

    except Exception as e:
        logging.error(f"！！！保存分析报告到文件失败: {str(e)}", exc_info=True)
        return False

def send_analysis_report(analysis):
    """发送分析报告到方糖"""
    if not FANGTANG_KEY:
        logging.warning("未设置方糖 KEY，跳过推送")
        return False
    
    try:
        china_time = get_china_time()
        time_str = china_time.strftime('%Y-%m-%d %H:%M')
        
        title = f"投资数据综合AI分析 - {time_str} (中国时间)"
        content = analysis
        short = "投资数据综合AI分析已生成"
        
        logging.info("准备发送报告到方糖...")
        return send_to_fangtang(title, content, short)
    
    except Exception as e:
        logging.error(f"发送分析报告失败: {str(e)}")
        return False

def process_all_data():
    """处理所有数据，生成、保存并发送综合分析"""
    logging.info("="*20 + " 开始处理所有数据 " + "="*20)
    
    logging.info("步骤 1/5: 加载研报数据...")
    reports_data = load_research_reports() or "暂无研报数据"
    logging.info(f"研报数据加载完毕. 内容摘要 (前100字符): {reports_data[:100].strip()}")
    
    logging.info("步骤 2/5: 加载财经新闻数据...")
    financial_news = load_financial_news()
    logging.info(f"财经新闻加载完毕. 内容摘要 (前100字符): {financial_news[:100].strip()}")

    logging.info("步骤 3/5: 加载财联社新闻数据...")
    cls_news = load_cls_news()
    logging.info(f"财联社新闻加载完毕. 内容摘要 (前100字符): {cls_news[:100].strip()}")
    
    logging.info("步骤 4/5: 加载国际市场数据...")
    market_data, market_analysis = load_market_data()
    market_analysis_str = str(market_analysis) if market_analysis else "暂无国际市场分析数据"
    logging.info(f"国际市场数据加载完毕. 内容摘要 (前100字符): {market_analysis_str[:100].strip()}")

    logging.info("步骤 5/5: 生成综合分析...")
    analysis = generate_comprehensive_analysis(reports_data, financial_news, cls_news, market_analysis_str)
    
    if analysis and "生成分析失败" not in analysis and len(analysis) > 50:
        logging.info(f"成功获取有效分析报告，长度为 {len(analysis)}。准备保存和推送。")
        
        # 1. 保存分析报告到文件
        logging.info("--- 开始保存文件流程 ---")
        save_success = save_analysis_to_file(analysis)
        if save_success:
            logging.info("文件保存流程成功结束。")
        else:
            logging.error("！！！文件保存流程失败，请检查上方日志。")

        # 2. 发送分析报告到方糖
        logging.info("--- 开始方糖推送流程 ---")
        push_success = send_analysis_report(analysis)
        if push_success:
            logging.info("方糖推送流程成功结束。")
        else:
            logging.error("！！！方糖推送流程失败。")
    else:
        logging.error(f"！！！未生成有效的综合分析报告，跳过保存和推送。获取到的内容: {analysis[:200]}")

    logging.info("="*20 + " 所有数据处理完毕 " + "="*20)


if __name__ == "__main__":
    logging.info("脚本启动...")
    process_all_data()
    logging.info("脚本执行完毕。")

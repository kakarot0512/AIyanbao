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
import re
import yfinance as yf
from datetime import datetime, timezone, timedelta
from functools import wraps
from tqdm import tqdm
import google.generativeai as genai
from google.generativeai import types

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
MARKET_DATA_DIR = "国际市场数据"  # 国际市场数据目录
DAILY_REPORT_DIR = "每日报告" # 每日报告输出目录
ANALYSIS_RESULT_DIR = "分析结果"  # Check分析结果目录
RAW_DATA_DIR = "股票原始数据"  # 股票原始数据目录

# 创建数据目录
os.makedirs(FINANCIAL_NEWS_DIR, exist_ok=True)
os.makedirs(MARKET_DATA_DIR, exist_ok=True)
os.makedirs(DAILY_REPORT_DIR, exist_ok=True)
os.makedirs(ANALYSIS_RESULT_DIR, exist_ok=True)
os.makedirs(RAW_DATA_DIR, exist_ok=True)

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
        # 获取当前年月（使用中国时间）
        current_date = get_china_time()
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
        # 获取当前周和上一周的信息（使用中国时间）
        current_date = get_china_time()
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

def load_market_data():
    """加载最新的国际市场数据"""
    try:
        # 尝试加载最新的市场数据和分析
        market_data_file = os.path.join(MARKET_DATA_DIR, "global_market_data_latest.json")
        market_analysis_file = os.path.join(MARKET_DATA_DIR, "market_analysis_" + get_china_time().strftime('%Y-%m-%d') + ".md")
        
        market_data = None
        market_analysis = None
        
        # 加载市场数据
        if os.path.exists(market_data_file):
            with open(market_data_file, 'r', encoding='utf-8') as f:
                market_data = json.load(f)
            logging.info(f"成功加载国际市场数据")
        else:
            logging.warning(f"国际市场数据文件 {market_data_file} 不存在")
        
        # 加载市场分析
        if os.path.exists(market_analysis_file):
            with open(market_analysis_file, 'r', encoding='utf-8') as f:
                market_analysis = f.read()
            logging.info(f"成功加载国际市场分析")
        else:
            # 尝试查找最近的分析文件
            analysis_files = [f for f in os.listdir(MARKET_DATA_DIR) if f.startswith("market_analysis_") and f.endswith(".md")]
            if analysis_files:
                # 按日期排序，获取最新的
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
        # 使用 Gemini 模型
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # 获取当前中国时间格式化字符串，包含星期
        china_time = get_china_time()
        weekday_names = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
        weekday = weekday_names[china_time.weekday()]
        current_time = f"{china_time.strftime('%Y年%m月%d日 %H:%M')} {weekday}"
        
        # 准备提示词
        prompt = ANALYST_PROMPT.format(
            reports_data=reports_data,
            financial_news=financial_news,
            cls_news=cls_news,
            current_time=current_time,
            market_analysis=market_analysis
        )
        
        # 生成分析
        logging.info("开始使用 Gemini 生成综合分析...")
        
        # 生成内容
        response = model.generate_content(prompt)
        
        if response and hasattr(response, 'text'):
            logging.info("成功生成综合分析")
            return response.text
        else:
            logging.error("生成分析失败: 响应格式异常或内容为空")
            # 尝试从 response.parts 获取内容
            try:
                return response.parts[0].text
            except (IndexError, AttributeError):
                logging.error("无法从 response.parts 提取文本")
                return "生成分析失败: 响应格式异常"

    except Exception as e:
        logging.error(f"生成分析失败: {str(e)}")
        return f"生成分析失败: {str(e)}"

def save_analysis_to_file(analysis_content):
    """将分析报告保存到指定目录的 MD 文件中"""
    try:
        # 获取当前中国时间
        china_time = get_china_time()
        date_str = china_time.strftime('%Y-%m-%d')
        time_str = china_time.strftime('%H%M%S')

        # 构建目录路径: 每日报告/YYYY-MM-DD/
        report_date_dir = os.path.join(DAILY_REPORT_DIR, date_str)
        os.makedirs(report_date_dir, exist_ok=True)

        # 构建完整文件路径
        file_name = f"analysis_report_{time_str}.md"
        file_path = os.path.join(report_date_dir, file_name)

        # 写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(analysis_content)

        logging.info(f"分析报告已成功保存到: {file_path}")
        return file_path

    except Exception as e:
        logging.error(f"保存分析报告到文件失败: {str(e)}")
        return None

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
    """处理所有数据，生成、保存并发送综合分析"""
    logging.info("开始处理所有数据...")
    
    # 加载研报数据
    reports_data = load_research_reports() or "暂无研报数据"
    
    # 加载财经新闻数据
    financial_news = load_financial_news()
    
    # 加载财联社新闻数据
    cls_news = load_cls_news()
    
    # 加载国际市场数据
    market_data, market_analysis = load_market_data()
    market_analysis_str = str(market_analysis) if market_analysis else "暂无国际市场分析数据"
    
    # 生成综合分析
    analysis = generate_comprehensive_analysis(reports_data, financial_news, cls_news, market_analysis_str)
    
    # 只有在成功生成分析报告时才进行保存和推送
    if analysis and "生成分析失败" not in analysis:
        # 1. 保存分析报告到文件
        saved_file_path = save_analysis_to_file(analysis)
        if not saved_file_path:
            logging.error("分析报告保存失败，但仍会尝试推送。")

        # 2. 进行股票筛选分析并补充到报告
        if saved_file_path and os.path.exists(saved_file_path):
            logging.info("开始进行股票筛选分析...")
            check_analysis_success = perform_stock_check_analysis(saved_file_path)
            if not check_analysis_success:
                logging.error("股票筛选分析失败，但仍会继续推送原始报告")

        # 3. 发送分析报告到方糖
        # 重新读取可能已经补充的报告内容
        final_analysis = analysis
        if saved_file_path and os.path.exists(saved_file_path):
            try:
                with open(saved_file_path, 'r', encoding='utf-8') as f:
                    final_analysis = f.read()
            except Exception as e:
                logging.error(f"读取补充后的报告失败: {e}")
        
        push_success = send_analysis_report(final_analysis)
        if push_success:
            logging.info("综合分析报告已成功推送")
        else:
            logging.error("综合分析报告推送失败")
    else:
        logging.error("未生成有效的综合分析报告，跳过保存和推送")

# === Check.py 集成功能 ===

def retry(retries=3, delay=5, backoff=2):
    """
    一个带指数退避的重试装饰器，用于网络请求。
    这有助于处理临时的网络问题或API速率限制。
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _retries, _delay = retries, delay
            while _retries > 0:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    _retries -= 1
                    if _retries == 0:
                        logging.warning(f"函数 {func.__name__} 在所有重试后失败: {e}")
                        return None
                    logging.warning(f"函数 {func.__name__} 失败: {e}. 将在 {_delay} 秒后重试... ({_retries} 次重试剩余)")
                    time.sleep(_delay)
                    _delay *= backoff
        return wrapper
    return decorator

def extract_stock_recommendations(report_path):
    """从 Markdown 报告中解析并提取推荐的股票列表。"""
    logging.info(f"正在从报告中提取股票推荐: {report_path}...")
    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 正则表达式寻找Markdown表格
        table_pattern = re.compile(r"\|\s*股票代码\s*\|\s*公司名称\s*\|.*?\n(?:\|:?-+:?\|:?-+:?\|.*?\n)((?:\|.*?\|\n)+)", re.DOTALL)
        match = table_pattern.search(content)
        if not match:
            logging.error("在报告中找不到格式正确的股票推荐表。")
            return None
        
        table_content = match.group(1).strip()
        rows = [row for row in table_content.split('\n') if row.strip()]
        stocks = []
        for row in rows:
            cells = [c.strip() for c in row.split('|') if c.strip()]
            # 仅提取6位纯数字的股票代码
            if len(cells) >= 2 and re.match(r'^\d{6}$', cells[0]):
                stocks.append({'code': cells[0], 'name': cells[1]})

        if not stocks:
            logging.error("未能从表中解析任何有效的股票。")
            return None
            
        logging.info(f"成功提取 {len(stocks)} 支推荐股票。")
        return stocks
    except Exception as e:
        logging.error(f"提取股票推荐时发生错误: {e}")
        return None

def get_yahoo_ticker_symbol(code):
    """为雅虎财经自动添加正确的交易所后缀。"""
    code_str = str(code).strip()
    if code_str.startswith(('60', '68')): # 沪市主板、科创板
        return f"{code_str}.SS"
    elif code_str.startswith(('00', '30', '20')): # 深市主板、创业板
        return f"{code_str}.SZ"
    else:
        logging.warning(f"无法确定股票代码 {code_str} 的交易所。将默认尝试上交所 (.SS)。")
        return f"{code_str}.SS"

@retry(retries=3, delay=10)
def fetch_stock_data_from_yahoo(stock_info):
    """从Yahoo Finance获取一支股票的股价和财务数据，并保存到本地。"""
    code = stock_info['code']
    name = stock_info['name']
    ticker_symbol = get_yahoo_ticker_symbol(code)
    stock_yf = yf.Ticker(ticker_symbol)

    stock_data_package = {"code": code, "name": name}

    # 为当前股票创建独立的原始数据存储目录
    stock_raw_data_dir = os.path.join(RAW_DATA_DIR, code)
    os.makedirs(stock_raw_data_dir, exist_ok=True)

    # 1. 获取最近半年的股价历史
    hist_data = stock_yf.history(period="6mo")
    if hist_data.empty:
        logging.warning(f"未能获取 {name}({code}) 的股价历史数据。")
        return None # 如果没有股价数据，则跳过此股票
    
    # 按时间降序排序 (最新日期在前)
    hist_data.sort_index(ascending=False, inplace=True)
    
    # 保存原始股价数据到CSV
    hist_data.to_csv(os.path.join(stock_raw_data_dir, f"{code}_price_6mo.csv"))
    
    # 替换NaN为None，并将Date索引转为列，以便JSON序列化用于AI分析
    hist_data.index = hist_data.index.strftime('%Y-%m-%d')
    hist_data = hist_data.where(pd.notnull(hist_data), None)
    stock_data_package["price_data"] = hist_data.reset_index().to_dict('records')

    # 2. 获取财务报表 (最近四年)
    try:
        # 获取年度数据
        income_stmt = stock_yf.income_stmt.iloc[:, :4]
        balance_sheet = stock_yf.balance_sheet.iloc[:, :4]
        cash_flow = stock_yf.cashflow.iloc[:, :4]

        # 保存原始财务数据到CSV
        income_stmt.to_csv(os.path.join(stock_raw_data_dir, f"{code}_income_4y.csv"))
        balance_sheet.to_csv(os.path.join(stock_raw_data_dir, f"{code}_balance_4y.csv"))
        cash_flow.to_csv(os.path.join(stock_raw_data_dir, f"{code}_cashflow_4y.csv"))

        # 清理并格式化数据以用于AI分析
        def format_financial_statement(df):
            df.columns = df.columns.strftime('%Y-%m-%d')
            transposed_df = df.transpose()
            transposed_df = transposed_df.where(pd.notnull(transposed_df), None)
            return transposed_df.reset_index().to_dict('records')

        stock_data_package["income_statement"] = format_financial_statement(income_stmt)
        stock_data_package["balance_sheet"] = format_financial_statement(balance_sheet)
        stock_data_package["cash_flow"] = format_financial_statement(cash_flow)
        
        logging.info(f"已为股票 {code} 保存原始数据于 '{stock_raw_data_dir}'")

    except Exception as e:
        logging.warning(f"获取 {name}({code}) 的财务数据时出错 (可能是数据不完整): {e}。财务数据将为空。")
        stock_data_package["income_statement"] = []
        stock_data_package["balance_sheet"] = []
        stock_data_package["cash_flow"] = []

    return stock_data_package

@retry(retries=5, delay=10, backoff=2)
def analyze_stocks_in_batch(all_stock_data):
    """
    使用 Gemini AI 模型批量分析股票。
    一次性发送所有股票数据，并期望获得批量响应。
    """
    if not GEMINI_API_KEY:
        logging.warning("GEMINI_API_KEY 未设置，跳过 AI 分析。")
        return {}

    logging.info(f"正在使用 Gemini AI 批量分析 {len(all_stock_data)} 支股票...")

    prompt_header = """
# 角色
你是一位经验丰富、一丝不苟的A股股票分析师，擅长通过解读股价走势和财务报表来识别长期价值和潜在风险。

# 任务
根据下面提供的一组股票列表及其从Yahoo Finance获取的原始数据（近半年股价历史和近几年年度财务报表），你必须 **无一例外地** 对列表中的 **每一支** 股票进行综合分析。然后，根据以下核心标准，判断是否应该将其 **排除** 在一个观察列表中：

1.  **近期明显下跌走势**: 股票价格在近期（例如最近2个月）表现出清晰、持续的下跌趋势，没有企稳迹象。
2.  **涨幅巨大，炒作接近泡沫**: 股票价格在短期内（例如过去半年）已经经历了非常巨大的涨幅，估值可能过高，存在泡沫风险。
3.  **财务有严重问题**: 股票存在明显的财务风险，例如连续亏损、经营性现金流持续为负、负债率过高且持续恶化等。

# 特殊情况处理
- **数据不完整**: 如果某支股票的关键财务数据（如利润表、现金流量表）严重缺失，**不要将其排除**。在分析中明确指出数据不完整，并建议用户自行核实。
"""

    prompt_data_section = f"""
# 待分析数据
这是待分析的股票列表及其数据：
```json
{json.dumps(all_stock_data, ensure_ascii=False, indent=2, default=str)}
```
"""
    
    prompt_footer = """
# 分析与输出要求
1.  **严格遵守**: 你必须严格按照上述排除标准和特殊情况处理规则，对输入数据中的 **每一支股票** 进行分析并形成一个JSON对象。
2.  **完整性保证**: 返回的JSON数组 **必须** 包含与输入数据中相同数量的对象。**绝对不要遗漏任何一支股票**。
3.  **强制格式**: 数组中每个对象的结构 **必须** 如下所示。`code` 字段至关重要。
```json
{
  "code": "原始股票代码, 例如 '600519'",
  "should_exclude": boolean,
  "reason": "仅在 should_exclude 为 true 时填写此字段，从'近期明显下跌走势'、'涨幅巨大接近泡沫'、'财务有严重问题'中选择一个。如果should_exclude为false，此字段应为空字符串或null。",
  "analysis": "提供一句话的简明扼要的分析，解释你做出该判断的核心依据。对于数据不完整的股票，should_exclude应为false，并在此处说明'数据不完整，建议用户自行核实相关财务报表。'"
}
```
4.  **纯净输出**: 确保最终输出是一个格式良好的、完整的JSON数组，前后不要有任何其他文本或Markdown标记。
"""

    full_prompt = prompt_header + prompt_data_section + prompt_footer

    try:
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        logging.info("正在向 Gemini API 发送请求 (这可能需要一些时间)...")
        start_time = time.time()
        response = model.generate_content(full_prompt)
        elapsed_time = time.time() - start_time
        logging.info(f"在 {elapsed_time:.2f} 秒内收到 Gemini API 的响应")
        
        # 增加日志以进行调试
        logging.info(f"收到来自AI的原始响应文本: {response.text[:1000]}...")

        # 清理并解析JSON响应
        response_text = response.text.strip()
        # 更稳健地移除代码块标记
        match = re.search(r'```(json)?\n(.*)\n```', response_text, re.DOTALL)
        if match:
            response_text = match.group(2).strip()
        
        results_list = json.loads(response_text)
        
        if not isinstance(results_list, list):
            logging.error(f"响应格式无效: 期望是列表，但得到 {type(results_list)}")
            return {}
            
        # 将列表转换为以股票代码为键的字典，便于查找
        results_map = {str(result.get("code")): result for result in results_list if result.get("code")}
        
        # 检查是否有股票被遗漏
        if len(results_map) < len(all_stock_data):
            logging.warning(f"AI响应中遗漏了 {len(all_stock_data) - len(results_map)} 支股票的分析结果。")

        logging.info(f"批量分析完成。收到 {len(results_map)} 支股票的分析结果。")
        return results_map
        
    except json.JSONDecodeError as e:
        logging.error(f"解析JSON响应失败: {e}")
        logging.error(f"失败的响应文本 (前500字符): {response.text[:500]}...")
        return {}
    except Exception as e:
        logging.error(f"批量AI分析期间发生严重错误: {e}")
        return {}

def save_stock_analysis_results(all_stock_data):
    """将最终的分析结果保存为JSON和Markdown文件。"""
    logging.info("正在保存所有分析结果...")
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_data = [{"code": s["code"], "name": s["name"], "analysis_result": s.get("analysis_result", {})} for s in all_stock_data]
    
    # 保存为JSON
    json_path = os.path.join(ANALYSIS_RESULT_DIR, f"stock_analysis_{current_time}.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)
    
    # 保存为Markdown报告
    md_path = os.path.join(ANALYSIS_RESULT_DIR, f"stock_analysis_{current_time}.md")
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(f"# 股票筛选分析报告 (AI-Powered)\n\n**分析时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        included = [s for s in report_data if not s.get('analysis_result', {}).get('should_exclude')]
        excluded = [s for s in report_data if s.get('analysis_result', {}).get('should_exclude')]
        
        f.write(f"## 筛选摘要\n\n- **总分析股票数:** {len(report_data)}\n- **建议保留:** {len(included)}\n- **建议排除:** {len(excluded)}\n\n")
        
        f.write("## 建议保留的股票\n\n")
        if included:
            f.write("| 股票代码 | 公司名称 | AI分析摘要 |\n|:---:|:---:|:---|\n")
            for stock in included:
                analysis_text = stock['analysis_result'].get('analysis', 'N/A')
                # 如果包含建议核实的文本，则加粗以突出显示
                if "建议用户自行核实" in analysis_text:
                    analysis_text = f"**{analysis_text}**"
                f.write(f"| {stock['code']} | {stock['name']} | {analysis_text} |\n")
        else:
            f.write("无。\n\n")
        
        f.write("\n## 建议排除的股票\n\n")
        if excluded:
            f.write("| 股票代码 | 公司名称 | 排除原因 | AI分析摘要 |\n|:---:|:---:|:---:|:---|\n")
            for stock in excluded:
                f.write(f"| {stock['code']} | {stock['name']} | **{stock['analysis_result'].get('reason', 'N/A')}** | {stock['analysis_result'].get('analysis', 'N/A')} |\n")
        else:
            f.write("无。\n\n")
    
    logging.info(f"分析结果已保存至 {json_path} 和 {md_path}")
    return md_path

def perform_stock_check_analysis(report_file_path):
    """执行股票筛选分析并将结果补充到报告文件"""
    try:
        logging.info("===== 开始股票筛选工作流 (Yahoo Finance + AI批量分析模式) =====")
        
        if not GEMINI_API_KEY:
            logging.error("Gemini API 未启用或配置失败，无法执行分析。")
            return False

        # 1. 从报告中提取股票列表
        stocks_to_analyze = extract_stock_recommendations(report_file_path)
        if not stocks_to_analyze:
            logging.error("无法从报告中提取股票列表，跳过股票筛选分析。")
            return False
        
        # --- 步骤 1: 从Yahoo Finance获取数据 ---
        logging.info("\n--- 步骤 1: 从 Yahoo Finance 获取所有股票的数据 ---")
        all_stock_data = []
        
        for stock in tqdm(stocks_to_analyze, desc="获取并保存雅虎财经数据"):
            stock_data_package = fetch_stock_data_from_yahoo(stock)
            if stock_data_package:
                all_stock_data.append(stock_data_package)
            else:
                logging.warning(f"无法获取股票 {stock['code']} 的数据，将在分析中跳过。")

        if not all_stock_data:
            logging.error("未能成功获取任何股票的数据，跳过股票筛选分析。")
            return False

        logging.info(f"成功获取了 {len(all_stock_data)} / {len(stocks_to_analyze)} 支股票的数据。")
        
        # --- 步骤 2: AI 批量分析 ---
        logging.info("\n--- 步骤 2: 开始 AI 批量分析 ---")
        
        analysis_results_map = analyze_stocks_in_batch(all_stock_data)
        
        # 为未收到分析结果的股票设置默认错误信息
        error_result = {"should_exclude": True, "reason": "分析失败", "analysis": "未能从AI获取对此股票的有效分析。检查日志中AI的原始响应。"}
        
        # 将分析结果映射回原始数据列表
        for stock_data in all_stock_data:
            stock_code = stock_data['code']
            stock_data['analysis_result'] = analysis_results_map.get(stock_code, error_result)

        logging.info("已成功映射所有AI分析结果。")
        
        # --- 步骤 3: 保存股票筛选报告 ---
        logging.info("\n--- 步骤 3: 保存股票筛选分析报告 ---")
        stock_analysis_report_path = save_stock_analysis_results(all_stock_data)
        
        # --- 步骤 4: 将股票筛选结果补充到原始报告 ---
        logging.info("\n--- 步骤 4: 将股票筛选结果补充到原始报告 ---")
        if stock_analysis_report_path and os.path.exists(stock_analysis_report_path):
            try:
                # 读取股票筛选分析结果
                with open(stock_analysis_report_path, 'r', encoding='utf-8') as f:
                    stock_analysis_content = f.read()
                
                # 读取原始报告
                with open(report_file_path, 'r', encoding='utf-8') as f:
                    original_report = f.read()
                
                # 将股票筛选分析结果补充到原始报告末尾
                supplemented_report = original_report + "\n\n---\n\n" + stock_analysis_content
                
                # 写回原始报告文件
                with open(report_file_path, 'w', encoding='utf-8') as f:
                    f.write(supplemented_report)
                
                logging.info(f"股票筛选分析结果已成功补充到原始报告: {report_file_path}")
                
            except Exception as e:
                logging.error(f"补充股票筛选分析结果到原始报告失败: {e}")
                return False
        
        logging.info("===== 股票筛选工作流成功完成 =====")
        return True
        
    except Exception as e:
        logging.error(f"股票筛选分析过程中发生严重错误: {e}")
        return False

if __name__ == "__main__":
    # 处理所有数据
    process_all_data()

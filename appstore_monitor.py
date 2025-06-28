import requests
import time
import logging
import os
import sys
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
import google.generativeai as genai

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 配置参数
DEFAULT_COUNTRY = "cn"  # Default country if not specified
FANGTANG_KEY = os.environ.get("FANGTANG_KEY", "")  # 从环境变量获取方糖 KEY
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")  # 从环境变量获取 Gemini API KEY
APP_INFO_FILE = "app_info.json"  # 应用信息 JSON 文件
STATUS_FILE = "app_status.json"  # 应用状态记录文件
LATEST_REPORT_FILE = "研报数据/慧博研报_最新数据.csv"  # 最新研报数据文件

# 配置 Gemini API
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    
# 研报分析师角色描述
ANALYST_PROMPT = """
# 角色
你是一位拥有15年经验的中国A股基金经理和首席投资策略分析师，尤其擅长从海量、混杂的券商研报和市场信息中，通过交叉验证和逻辑推演，挖掘出具备"预期差"和高成长潜力的投资机会。

# 背景
你获得了近期发布的一批A股券商研究报告作为初步信息源。你知道这些报告观点可能存在滞后性、片面性甚至错误，因此你的核心价值在于独立思考和深度甄别，而非简单复述。

# 任务
请你基于下面提供的参考资料，严格遵循以下分析框架，为我构建并详细阐述一个由8-12只A股组成的【高成长潜力模拟投资组合】。

**分析框架 (请严格按步骤执行):**

1.  **宏观主题识别 (Theme Identification):**
    * 快速扫描所有研报摘要，识别并归纳出当前市场关注度最高、被多家券商反复提及的2-4个核心投资主题或赛道（例如：AI硬件、出海龙头、机器人产业链、半导体国产化、消费电子复苏等）。

2.  **多源交叉验证 (Cross-Validation):**
    * 在识别出的每个核心主题下，筛选出被 **至少2家或以上不同券商** 同时给予"买入"、"增持"或同等正面评级的个股，形成初步候选池。
    * 对比不同研报对同一家公司的核心观点，标记出其中的 **共识（Consensus）** 与 **分歧（Divergence）**。共识部分是投资逻辑的基石，分歧部分则可能隐藏着风险或超额收益的机会。

3.  **个股深度剖析 (Deep Dive Analysis):**
    * 从候选池中，基于以下标准挑选最终入选组合的个股：
        * **成长驱动力清晰**: 公司的主营业务增长逻辑是否强劲且可持续？（例如：技术突破、新订单、产能扩张、市占率提升）。
        * **业绩可见性高**: 研报中是否提及具体的业绩预告、订单合同、或明确的业绩改善信号？
        * **估值相对合理**: 虽然是成长组合，但其估值是否在同业或历史中具有相对吸引力？(可基于研报摘要信息做初步判断)

4.  **投资组合构建与风险管理 (Portfolio Construction & Risk Management):**
    * 最终构建一个包含8-12只股票的投资组合。
    * 组合内应适当分散，覆盖你识别出的主要核心主题，避免在单一赛道上过度集中。
    * 为每只入选的股票，明确其在组合中的定位（例如："核心配置"代表逻辑最强、确定性高；"卫星配置"代表弹性较大、属于博取更高收益的部分）。

**输出格式 (请严格按照以下结构呈现):**

**一、 市场核心洞察与投资策略**
* （简要总结你从这批研报中感知到的整体市场情绪、热点板块轮动特征，以及你本次构建组合的核心策略。）

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
{reports_data}
"""

def load_app_info():
    """从 JSON 文件加载应用信息"""
    try:
        with open(APP_INFO_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Handle different format versions
        if isinstance(data, list):
            # New array format - each app has its own countries array
            return data
        elif isinstance(data, dict) and "apps" in data:
            # Old format with default_country - convert to new format
            default_country = data.get("default_country", DEFAULT_COUNTRY)
            new_format = []
            
            for app in data["apps"]:
                # If app has country property, use it, otherwise use default
                countries = [app.get("country", default_country)]
                new_app = {
                    "id": app["id"],
                    "name": app["name"],
                    "countries": countries
                }
                new_format.append(new_app)
            
            return new_format
        else:
            logging.error("未知的应用信息格式")
            return []
    except Exception as e:
        logging.error(f"加载应用信息文件失败: {str(e)}")
        return []

def load_app_status():
    """加载上次的应用状态"""
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                status_data = json.load(f)
            
            # Check if we need to migrate from old format to new format
            # Old format used app_id as key, new format uses app_id_country
            needs_migration = True
            for key in status_data:
                if '_' in key:  # New format already has compound keys with underscore
                    needs_migration = False
                    break
            
            if needs_migration:
                logging.info("检测到旧格式的状态文件，进行自动迁移...")
                migrated_data = {}
                
                # Get the current app configuration for country info
                app_info = load_app_info()
                app_country_map = {}
                
                # Build a mapping of app_id to countries
                for app in app_info:
                    app_id = app["id"]
                    countries = app.get("countries", [DEFAULT_COUNTRY])
                    app_country_map[app_id] = countries
                
                # Convert each app status to the new format
                for app_id, status in status_data.items():
                    # If app exists in current config, use its countries, otherwise default to "cn"
                    countries = app_country_map.get(app_id, [DEFAULT_COUNTRY])
                    
                    for country in countries:
                        new_key = f"{app_id}_{country}"
                        migrated_data[new_key] = {
                            "status": status.get("status", "unknown"),
                            "name": status.get("name", "Unknown App"),
                            "country": country,
                            "app_id": app_id,
                            "last_check": status.get("last_check", "未检查")
                        }
                
                # Save the migrated data
                with open(STATUS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(migrated_data, f, ensure_ascii=False, indent=2)
                logging.info("状态文件迁移完成")
                
                return migrated_data
            
            return status_data
        # 如果文件不存在，创建一个初始状态文件
        else:
            logging.info(f"状态文件 {STATUS_FILE} 不存在，将创建初始状态文件")
            create_initial_status_file()
            with open(STATUS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"加载应用状态文件失败: {str(e)}")
        return {}

def create_initial_status_file():
    """创建初始状态文件"""
    try:
        app_info = load_app_info()
        initial_status = {}
        
        for app in app_info:
            app_id = app["id"]
            app_name = app["name"]
            countries = app.get("countries", [DEFAULT_COUNTRY])
            
            # 为每个应用的每个国家/地区创建状态
            for country in countries:
                status_key = f"{app_id}_{country}"
                initial_status[status_key] = {
                    "status": "unknown",  # 初始状态为未知
                    "name": app_name,
                    "country": country,
                    "app_id": app_id,
                    "last_check": "未检查"
                }
        
        with open(STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(initial_status, f, ensure_ascii=False, indent=2)
        logging.info(f"已创建初始状态文件 {STATUS_FILE}")
    except Exception as e:
        logging.error(f"创建初始状态文件失败: {str(e)}")

def save_app_status(status_dict):
    """保存应用状态"""
    try:
        with open(STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status_dict, f, ensure_ascii=False, indent=2)
        logging.info("应用状态已保存")
    except Exception as e:
        logging.error(f"保存应用状态失败: {str(e)}")

def get_app_info(app_id: str, default_name: str, country_code: str = DEFAULT_COUNTRY) -> dict:
    """通过 App ID 获取应用信息"""
    try:
        params = {"id": app_id, "country": country_code}
        logging.info(f"查询应用 ID: {app_id}, 国家/地区: {country_code}")
        response = requests.get("https://itunes.apple.com/lookup", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data["resultCount"] > 0:
            result = data["results"][0]
            return {
                "status": "online",
                "name": result.get("trackName", default_name),
                "developer": result.get("sellerName", "未知开发者"),  # 获取开发者名称
                "version": result.get("version", "未知"),
                "price": result.get("formattedPrice", "未知"),
                "url": result.get("trackViewUrl", ""),
                "country": country_code,
                "app_id": app_id
            }
        return {"status": "offline", "name": default_name, "developer": "未知开发者", "country": country_code, "app_id": app_id}
    
    except Exception as e:
        logging.error(f"查询 {app_id} (国家/地区: {country_code}) 失败: {str(e)}")
        return {"status": "error", "name": default_name, "developer": "未知开发者", "country": country_code, "app_id": app_id}

def format_app_detail(info):
    """格式化应用详细信息"""
    status_icon = "✅" if info["status"] == "online" else "🚫" if info["status"] == "offline" else "❌"
    
    country = info["country"].upper()
    app_id = info["app_id"]
    developer = info.get("developer", "未知开发者")
    
    # 简洁格式，显示状态、ID、名称、开发者和国家/地区
    return f"{status_icon} **{info['name']}** (开发者: {developer}, ID: {app_id}, 区域: {country})"

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

def get_china_time():
    """获取中国时间"""
    # 获取当前 UTC 时间
    utc_now = datetime.now(timezone.utc)
    # 转换为中国时间 (UTC+8)
    china_now = utc_now + timedelta(hours=8)
    return china_now

def is_within_time_range():
    """检查当前是否在中国时间 8:00-22:00 范围内"""
    # 获取中国时间
    china_now = get_china_time()
    # 提取小时
    hour = china_now.hour
    # 检查是否在 8-22 点之间
    return 8 <= hour < 22

def format_app_detail(info):
    """格式化应用详细信息"""
    status_icon = "✅" if info["status"] == "online" else "🚫" if info["status"] == "offline" else "❌"
    
    country = info["country"].upper()
    app_id = info["app_id"]
    
    # 简洁格式，显示状态、ID、名称和国家/地区
    return f"{status_icon} **{info['name']}** (ID: {app_id}, 区域: {country})"

def send_offline_alert(newly_offline_apps):
    """发送应用下架警告"""
    if not newly_offline_apps:
        return
    
    # 获取中国时间并格式化
    china_time = get_china_time()
    time_str = china_time.strftime('%H:%M')
    
    # 构建警告标题和内容
    title = f"⚠️ 应用下架警告 - {time_str} (中国时间)"
    content = "## 🚨 以下应用刚刚下架\n\n"
    
    for app in newly_offline_apps:
        country = app["country"].upper()
        app_id = app["app_id"]
        content += f"🚫 **{app['name']}** (ID: {app_id}, 区域: {country})\n\n"
    
    # 构建消息卡片内容
    short = f"有 {len(newly_offline_apps)} 个应用刚刚下架！"
    
    # 发送到方糖
    send_to_fangtang(title, content, short)
    logging.warning(f"已发送 {len(newly_offline_apps)} 个应用的下架警告")

def monitor(force_send=False):
    """执行监控任务"""
    # 如果不是强制发送且不在时间范围内，则跳过
    if not force_send and not is_within_time_range():
        logging.info("当前不在推送时间范围内 (中国时间 8:00-22:00)")
        return
    
    logging.info("开始检查应用状态")
    
    # 加载应用信息和上次状态
    app_info = load_app_info()
    if not app_info:
        logging.error("没有找到应用信息，请检查 app_info.json 文件")
        return
    
    previous_status = load_app_status()
    current_status = {}  # 用于保存本次检查的状态
    
    # 构建消息内容
    online_apps = []
    offline_apps = []
    error_apps = []
    newly_offline_apps = []  # 新下架的应用
    
    # 添加区域统计
    region_stats = {
        "cn": {"online": 0, "offline": 0, "error": 0},
        "us": {"online": 0, "offline": 0, "error": 0}
    }
    
    # 遍历每个应用及其指定的国家/地区
    for app in app_info:
        app_id = app["id"]
        default_name = app["name"]
        # 获取应用需要检查的国家/地区列表
        countries = app.get("countries", [DEFAULT_COUNTRY])
        
        for country in countries:
            # 为每个应用+国家组合生成唯一的状态键
            status_key = f"{app_id}_{country}"
            
            # 查询应用状态
            info = get_app_info(app_id, default_name, country)
            
            # 保存当前状态
            current_status[status_key] = {
                "status": info["status"],
                "name": info["name"],
                "country": country,
                "app_id": app_id,
                "last_check": get_china_time().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 检查是否新下架
            if (info["status"] == "offline" and 
                status_key in previous_status and 
                previous_status[status_key].get("status") == "online"):
                newly_offline_apps.append(info)
            
            # 按状态分类
            if info["status"] == "online":
                online_apps.append(format_app_detail(info))
                region_stats[country]["online"] += 1
                logging.info(f"✅ [ID: {app_id}] 名称: {info['name']} 区域: {country.upper()}")
            elif info["status"] == "offline":
                offline_apps.append(format_app_detail(info))
                region_stats[country]["offline"] += 1
                logging.warning(f"🚨 [ID: {app_id}] 应用已下架！名称: {info['name']} 区域: {country.upper()}")
            else:
                error_apps.append(format_app_detail(info))
                region_stats[country]["error"] += 1
                logging.error(f"❌ [ID: {app_id}] 查询异常，名称: {info['name']} 区域: {country.upper()}")
    
    # 保存当前状态
    save_app_status(current_status)
    
    # 如果有新下架的应用，发送警告
    if newly_offline_apps:
        send_offline_alert(newly_offline_apps)
    
    # 获取中国时间并格式化
    china_time = get_china_time()
    time_str = china_time.strftime('%H:%M')
    
    # 构建推送内容
    title = f"AppStore 监控报告 - {time_str} (中国时间)"
    
    # 添加区域统计信息
    content = "## 📊 区域统计\n\n"
    content += f"🇨🇳 中国区：在线 {region_stats['cn']['online']} 款 | 下架 {region_stats['cn']['offline']} 款"
    if region_stats['cn']['error'] > 0:
        content += f" | 异常 {region_stats['cn']['error']} 款"
    content += "\n\n"
    
    content += f"🇺🇸 美国区：在线 {region_stats['us']['online']} 款 | 下架 {region_stats['us']['offline']} 款"
    if region_stats['us']['error'] > 0:
        content += f" | 异常 {region_stats['us']['error']} 款"
    content += "\n\n"
    
    # 添加应用详细信息
    if online_apps:
        content += "## 📱 在线应用\n\n"
        
        # 按区域分组应用
        cn_apps = [app for app in online_apps if "区域: CN" in app]
        us_apps = [app for app in online_apps if "区域: US" in app]
        
        if cn_apps:
            content += "### 🇨🇳 中国区\n\n"
            for i, app in enumerate(cn_apps, 1):
                content += f"{app}\n\n"
                if i % 5 == 0 and i < len(cn_apps):
                    content += "---\n\n"
        
        if us_apps:
            content += "### 🇺🇸 美国区\n\n"
            for i, app in enumerate(us_apps, 1):
                content += f"{app}\n\n"
                if i % 5 == 0 and i < len(us_apps):
                    content += "---\n\n"
    
    if offline_apps:
        content += "## 🚫 已下架应用\n\n"
        
        # 按区域分组下架应用
        cn_offline = [app for app in offline_apps if "区域: CN" in app]
        us_offline = [app for app in offline_apps if "区域: US" in app]
        
        if cn_offline:
            content += "### 🇨🇳 中国区\n\n"
            for i, app in enumerate(cn_offline, 1):
                content += f"{app}\n\n"
                if i % 5 == 0 and i < len(cn_offline):
                    content += "---\n\n"
        
        if us_offline:
            content += "### 🇺🇸 美国区\n\n"
            for i, app in enumerate(us_offline, 1):
                content += f"{app}\n\n"
                if i % 5 == 0 and i < len(us_offline):
                    content += "---\n\n"

    if error_apps:
        content += "## ❌ 查询异常\n\n"
        
        # 按区域分组异常应用
        cn_error = [app for app in error_apps if "区域: CN" in app]
        us_error = [app for app in error_apps if "区域: US" in app]
        
        if cn_error:
            content += "### 🇨🇳 中国区\n\n"
            for i, app in enumerate(cn_error, 1):
                content += f"{app}\n\n"
                if i % 5 == 0 and i < len(cn_error):
                    content += "---\n\n"
        
        if us_error:
            content += "### 🇺🇸 美国区\n\n"
            for i, app in enumerate(us_error, 1):
                content += f"{app}\n\n"
                if i % 5 == 0 and i < len(us_error):
                    content += "---\n\n"

    # 构建消息卡片内容
    online_count = len(online_apps)
    offline_count = len(offline_apps)
    error_count = len(error_apps)
    
    short = f"CN区在线: {region_stats['cn']['online']} | US区在线: {region_stats['us']['online']}"
    if offline_count > 0:
        short += f" | 下架: {offline_count}"
    if error_count > 0:
        short += f" | 异常: {error_count}"
    
    # 发送到方糖
    send_to_fangtang(title, content, short)
    logging.info("本轮检查完成")

def load_research_reports():
    """加载最新的研报数据"""
    try:
        if os.path.exists(LATEST_REPORT_FILE):
            df = pd.read_csv(LATEST_REPORT_FILE, encoding='utf-8-sig')
            logging.info(f"成功加载研报数据，共 {len(df)} 条记录")
            return df
        else:
            logging.warning(f"研报数据文件 {LATEST_REPORT_FILE} 不存在")
            return None
    except Exception as e:
        logging.error(f"加载研报数据失败: {str(e)}")
        return None

def prepare_report_data_for_summary(df, max_reports=100):
    """准备研报数据用于生成摘要"""
    if df is None or len(df) == 0:
        return "没有可用的研报数据"
    
    # 限制研报数量，避免超出token限制
    if len(df) > max_reports:
        df = df.sample(max_reports)
        logging.info(f"随机抽取 {max_reports} 条研报数据用于生成摘要")
    
    # 构建研报数据文本
    report_texts = []
    
    for _, row in df.iterrows():
        report_text = f"研报标题: {row.get('研报标题', 'N/A')}\n"
        
        if '摘要' in row and pd.notna(row['摘要']) and row['摘要'] != 'N/A':
            report_text += f"摘要: {row['摘要']}\n"
        
        if '作者' in row and pd.notna(row['作者']) and row['作者'] != 'N/A':
            report_text += f"作者: {row['作者']}\n"
        
        if '评级' in row and pd.notna(row['评级']) and row['评级'] != 'N/A':
            report_text += f"评级: {row['评级']}\n"
        
        if '日期' in row and pd.notna(row['日期']) and row['日期'] != 'N/A':
            report_text += f"日期: {row['日期']}\n"
        
        if '分类' in row and pd.notna(row['分类']) and row['分类'] != 'N/A':
            report_text += f"分类: {row['分类']}\n"
        
        report_texts.append(report_text + "\n---\n")
    
    return "\n".join(report_texts)

def generate_report_summary(reports_data):
    """使用 Gemini 模型生成研报摘要"""
    if not GEMINI_API_KEY:
        logging.warning("未设置 Gemini API KEY，跳过生成摘要")
        return "未配置 Gemini API KEY，无法生成摘要"
    
    try:
        # 使用 Gemini 模型
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        # 准备提示词
        prompt = ANALYST_PROMPT.format(reports_data=reports_data)
        
        # 生成摘要
        logging.info("开始使用 Gemini 生成研报摘要...")
        response = model.generate_content(prompt)
        
        if response and hasattr(response, 'text'):
            logging.info("成功生成研报摘要")
            return response.text
        else:
            logging.error("生成摘要失败: 响应格式异常")
            return "生成摘要失败: 响应格式异常"
    
    except Exception as e:
        logging.error(f"生成摘要失败: {str(e)}")
        return f"生成摘要失败: {str(e)}"

def send_report_summary(summary):
    """发送研报摘要到方糖"""
    if not FANGTANG_KEY:
        logging.warning("未设置方糖 KEY，跳过推送")
        return False
    
    try:
        # 获取中国时间并格式化
        china_time = get_china_time()
        time_str = china_time.strftime('%Y-%m-%d %H:%M')
        
        # 构建推送标题和内容
        title = f"慧博研报AI分析 - {time_str} (中国时间)"
        content = summary
        short = "慧博研报AI分析已生成"
        
        # 发送到方糖
        return send_to_fangtang(title, content, short)
    
    except Exception as e:
        logging.error(f"发送研报摘要失败: {str(e)}")
        return False

def process_research_reports():
    """处理研报数据并发送摘要"""
    logging.info("开始处理研报数据...")
    
    # 加载研报数据
    df = load_research_reports()
    if df is None or len(df) == 0:
        logging.warning("没有可用的研报数据，跳过处理")
        return
    
    # 准备研报数据用于生成摘要
    reports_data = prepare_report_data_for_summary(df)
    
    # 生成摘要
    summary = generate_report_summary(reports_data)
    
    # 发送摘要
    if summary:
        success = send_report_summary(summary)
        if success:
            logging.info("研报摘要已成功推送")
        else:
            logging.error("研报摘要推送失败")
    else:
        logging.error("未生成研报摘要，跳过推送")

if __name__ == "__main__":
    # 检查是否有命令行参数
    force_send = len(sys.argv) > 1 and sys.argv[1] == "--force"
    
    # 检查是否需要处理研报数据
    process_reports = len(sys.argv) > 1 and sys.argv[1] == "--reports"
    
    if process_reports:
        # 只处理研报数据
        process_research_reports()
    else:
        # 执行常规的 App Store 监控
        monitor(force_send)
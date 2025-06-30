#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import logging
import importlib.util
from datetime import datetime
import schedule

# --- 配置日志 ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s', # 增加日志级别显示
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 模块导入函数 ---
def import_module_from_file(file_path, module_name):
    """
    从文件路径动态导入模块。
    这允许我们运行其他.py文件中的函数。
    """
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None:
            logging.error(f"无法找到模块文件: {file_path}")
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except FileNotFoundError:
        logging.error(f"导入模块失败，文件未找到: {file_path}")
        return None
    except Exception as e:
        logging.error(f"导入模块 {file_path} 时发生未知错误: {e}")
        return None

# --- 各个子任务执行函数 ---

def run_hibor_crawler():
    """运行慧博研报爬虫"""
    logging.info("开始运行慧博研报爬虫...")
    try:
        hibor_module = import_module_from_file("慧博研报爬虫整合版.py", "hibor_crawler")
        if hibor_module:
            hibor_module.main()
            logging.info("慧博研报爬虫运行完成")
            return True
        return False
    except Exception as e:
        logging.error(f"运行慧博研报爬虫时出错: {e}")
        return False

def run_financial_news_crawler():
    """运行财经新闻爬虫"""
    logging.info("开始运行财经新闻爬虫...")
    try:
        news_module = import_module_from_file("financial_news_crawler.py", "financial_news_crawler")
        if news_module:
            news_module.get_recent_financial_news()
            logging.info("财经新闻爬虫运行完成")
            return True
        return False
    except Exception as e:
        logging.error(f"运行财经新闻爬虫时出错: {e}")
        return False

def run_cls_crawler():
    """运行财联社爬虫"""
    logging.info("开始运行财联社爬虫...")
    try:
        os.makedirs("财联社/output/cls", exist_ok=True)
        cls_module = import_module_from_file("cls_spider.py", "cls_spider")
        if cls_module:
            cls_module.main()
            logging.info("财联社爬虫运行完成")
            return True
        return False
    except Exception as e:
        logging.error(f"运行财联社爬虫时出错: {e}")
        return False

def run_market_data_collector():
    """运行国际市场数据收集器"""
    logging.info("开始运行国际市场数据收集器...")
    try:
        market_module = import_module_from_file("AKShareSummer.py", "market_collector")
        if market_module:
            market_module.run_market_analysis()
            logging.info("国际市场数据收集器运行完成")
            return True
        return False
    except Exception as e:
        logging.error(f"运行国际市场数据收集器时出错: {e}")
        return False

def run_ai_analysis():
    """
    运行AI分析 (包含Check功能)。
    注意：这里的文件名 'appstore_monitor.py' 对应您之前提供的主分析脚本。
    """
    logging.info("开始运行AI分析...")
    try:
        ai_module = import_module_from_file("appstore_monitor.py", "ai_analysis")
        if ai_module:
            ai_module.process_all_data()
            logging.info("AI分析运行完成")
            return True
        return False
    except Exception as e:
        logging.error(f"运行AI分析时出错: {e}")
        return False

# --- 任务流与调度 ---

def run_pipeline_job():
    """
    定义完整的单次任务流：按顺序运行所有爬虫和AI分析。
    """
    start_time = datetime.now()
    logging.info(f"=== {start_time.strftime('%Y-%m-%d %H:%M:%S')} | 开始执行完整数据采集与分析任务流 ===")
    
    # 依次运行各个数据采集模块
    hibor_success = run_hibor_crawler()
    time.sleep(2)
    
    news_success = run_financial_news_crawler()
    time.sleep(2)
    
    cls_success = run_cls_crawler()
    time.sleep(2)
    
    market_success = run_market_data_collector()
    time.sleep(2)
    
    # 如果至少有一个爬虫成功，则运行AI分析
    if any([hibor_success, news_success, cls_success, market_success]):
        logging.info("至少一个数据采集任务成功，准备启动AI分析。")
        ai_success = run_ai_analysis()
    else:
        logging.error("所有数据采集任务均失败，本次跳过AI分析。")
        ai_success = False
    
    # 输出本次任务总结
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"=== 任务流执行完毕 | 耗时: {duration} ===")
    logging.info(f"  - 慧博研报爬虫: {'成功' if hibor_success else '失败'}")
    logging.info(f"  - 财经新闻爬虫: {'成功' if news_success else '失败'}")
    logging.info(f"  - 财联社爬虫: {'成功' if cls_success else '失败'}")
    logging.info(f"  - 国际市场数据: {'成功' if market_success else '失败'}")
    logging.info(f"  - AI综合分析 (含Check): {'成功' if ai_success else '失败'}")
    logging.info("-" * 40)


def main():
    """
    主函数，用于设置和启动定时任务。
    """
    logging.info("脚本启动，开始设置定时任务...")
    
    # --- 在这里配置你的定时任务 ---
    # 示例1：每天早上 8:00 执行任务
    schedule.every().day.at("08:00").do(run_pipeline_job)
    
    # 示例2：每隔6小时执行一次
    # schedule.every(6).hours.do(run_pipeline_job)
    
    # 示例3：每个星期一的 09:00 执行
    # schedule.every().monday.at("09:00").do(run_pipeline_job)
    
    logging.info("任务调度设置完成。脚本将按计划执行。")
    logging.info(f"下一次计划运行时间: {schedule.next_run}")
    
    # --- 启动时立即执行一次任务（可选）---
    # 如果你想在脚本启动时就运行一次，而不是等到下一个计划时间点，请取消下面这行的注释。
    # run_pipeline_job()
    
    # --- 保持脚本运行以执行定时任务 ---
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()

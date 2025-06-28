#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import logging
import importlib.util
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def import_module_from_file(file_path, module_name):
    """从文件路径导入模块"""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def run_hibor_crawler():
    """运行慧博研报爬虫"""
    logging.info("开始运行慧博研报爬虫...")
    try:
        # 导入慧博研报爬虫模块
        hibor_module = import_module_from_file("慧博研报爬虫整合版.py", "hibor_crawler")
        # 运行主函数
        hibor_module.main()
        logging.info("慧博研报爬虫运行完成")
        return True
    except Exception as e:
        logging.error(f"运行慧博研报爬虫时出错: {e}")
        return False

def run_financial_news_crawler():
    """运行财经新闻爬虫"""
    logging.info("开始运行财经新闻爬虫...")
    try:
        # 导入财经新闻爬虫模块
        news_module = import_module_from_file("financial_news_crawler.py", "financial_news_crawler")
        # 运行获取新闻的函数
        news_module.get_recent_financial_news()
        logging.info("财经新闻爬虫运行完成")
        return True
    except Exception as e:
        logging.error(f"运行财经新闻爬虫时出错: {e}")
        return False

def run_cls_crawler():
    """运行财联社爬虫"""
    logging.info("开始运行财联社爬虫...")
    try:
        # 创建必要的目录
        os.makedirs("财联社/output/cls", exist_ok=True)
        
        # 导入财联社爬虫模块
        cls_module = import_module_from_file("cls_spider.py", "cls_spider")
        # 运行主函数
        cls_module.main()
        logging.info("财联社爬虫运行完成")
        return True
    except Exception as e:
        logging.error(f"运行财联社爬虫时出错: {e}")
        return False

def run_market_data_collector():
    """运行国际市场数据收集器"""
    logging.info("开始运行国际市场数据收集器...")
    try:
        # 导入国际市场数据收集模块
        market_module = import_module_from_file("AKShareSummer.py", "market_collector")
        # 运行市场分析函数
        market_module.run_market_analysis()
        logging.info("国际市场数据收集器运行完成")
        return True
    except Exception as e:
        logging.error(f"运行国际市场数据收集器时出错: {e}")
        return False

def run_ai_analysis():
    """运行AI分析"""
    logging.info("开始运行AI分析...")
    try:
        # 导入AI分析模块
        ai_module = import_module_from_file("appstore_monitor.py", "ai_analysis")
        # 运行处理所有数据的函数
        ai_module.process_all_data()
        logging.info("AI分析运行完成")
        return True
    except Exception as e:
        logging.error(f"运行AI分析时出错: {e}")
        return False

def main():
    """主函数，按顺序运行所有爬虫和AI分析"""
    start_time = datetime.now()
    logging.info(f"=== 开始全部数据采集与分析任务 === {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 运行慧博研报爬虫
    hibor_success = run_hibor_crawler()
    time.sleep(2)  # 短暂暂停
    
    # 2. 运行财经新闻爬虫
    news_success = run_financial_news_crawler()
    time.sleep(2)  # 短暂暂停
    
    # 3. 运行财联社爬虫
    cls_success = run_cls_crawler()
    time.sleep(2)  # 短暂暂停
    
    # 4. 运行国际市场数据收集器
    market_success = run_market_data_collector()
    time.sleep(2)  # 短暂暂停
    
    # 5. 如果至少有一个爬虫成功，则运行AI分析
    if hibor_success or news_success or cls_success or market_success:
        ai_success = run_ai_analysis()
    else:
        logging.error("所有爬虫均失败，跳过AI分析")
        ai_success = False
    
    # 6. 输出总结
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"=== 全部任务完成 === 耗时: {duration}")
    logging.info(f"慧博研报爬虫: {'成功' if hibor_success else '失败'}")
    logging.info(f"财经新闻爬虫: {'成功' if news_success else '失败'}")
    logging.info(f"财联社爬虫: {'成功' if cls_success else '失败'}")
    logging.info(f"国际市场数据收集: {'成功' if market_success else '失败'}")
    logging.info(f"AI分析: {'成功' if ai_success else '失败'}")

if __name__ == "__main__":
    main() 
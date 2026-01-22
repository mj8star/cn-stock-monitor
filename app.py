import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from datetime import datetime, timedelta
import subprocess
import sys

# --- 页面配置 ---
st.set_page_config(layout="wide", page_title="A监控 | 资产监控系统", page_icon="📈")

# --- 资产分类定义 ---
ASSET_GROUPS = {
    "大盘指数": ["上证指数", "深证成指"],
    "国内权益": ["沪深300ETF"],
    "跨境/全球": ["港股互联网", "纳指ETF", "标普500ETF", "日经225"],
    "大宗商品": ["黄金ETF"]
}

# --- 工具函数 ---
def format_unit(val, metric):
    """根据指标类型进行单位换算"""
    if metric == "amount":  # 成交额换算为亿/万
        if val >= 1e8: return f"{val/1e8:.2f}亿"
        if val >= 1e4: return f"{val/1e4:.2f}万"
        return f"{val:.2f}"
    return f"{val:.2f}"

def load_data(start_date, end_date):
    """从本地SQLite读取数据"""
    try:
        conn = sqlite3.connect("stock_data.db")
        query = f"SELECT * FROM daily_records WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

# --- 侧边栏控制层 ---
st.sidebar.title("🚀 监控控制台")

# 1. 数据同步按钮
if st.sidebar.button("🔄 同步最新数据 (T+1)"):
    with st.spinner("正在调用采集脚本..."):
        # 使用 uv 环境运行 collector.py
        result = subprocess.run(["uv", "run", "collector.py"], capture_output=True, text=True)
        if result.returncode == 0:
            st.sidebar.success("同步成功！")
            st.rerun()
        else:
            st.sidebar.error(f"同步失败: {result.stderr}")

st.sidebar.divider()

# 2. 日期范围
default_start = datetime.now() - timedelta(days=30)
date_range = st.sidebar.date_input("日期范围", [default_start, datetime.now()])

# 3. 分类与标的选择
st.sidebar.subheader("资产筛选")
selected_groups = st.sidebar.multiselect("选择资产类别", list(ASSET_GROUPS.keys()), default=list(ASSET_GROUPS.keys()))

# 根据选中的分类，动态生成待选标的
relevant_targets = []
for group in selected_groups:
    relevant_targets.extend(ASSET_GROUPS[group])

selected_targets = st.sidebar.multiselect("选择具体标的", relevant_targets, default=relevant_targets[:3])

# 4. 指标选择
metrics_map = {
    "涨跌幅 (%)": "pct_chg",
    "成交额 (元)": "amount",
    "换手率 (%)": "turnover_rate",
    "振幅 (%)": "amplitude",
    "量比 (VR)": "vol_ratio"
}
selected_metric_label = st.sidebar.selectbox("选择核心指标", list(metrics_map.keys()))
metric_col = metrics_map[selected_metric_label]

# --- 主界面展示层 ---
st.title("📊 A股/全球资产多维监控")

if len(date_range) != 2:
    st.info("请选择完整的日期范围")
    st.stop()

# 加载并过滤数据
df = load_data(date_range[0].strftime("%Y-%m-%d"), date_range[1].strftime("%Y-%m-%d"))

if df.empty:
    st.warning("⚠️ 数据库为空或当前范围无数据，请先点击左侧『同步最新数据』。")
else:
    # 过滤选中的标的
    plot_df = df[df['name'].isin(selected_targets)].sort_values('date')
    
    if plot_df.empty:
        st.info("💡 请在左侧勾选你想要观察的标的名称。")
    else:
        # 准备显示用的辅助列
        plot_df['display_val'] = plot_df[metric_col].apply(lambda x: format_unit(x, metric_col))

        # --- 图表 1: 核心趋势图 ---
        st.subheader(f"📈 {selected_metric_label} 走势对比")
        fig = px.line(
            plot_df, 
            x="date", 
            y=metric_col, 
            color="name",
            markers=True,
            line_shape="linear",
            hover_name="name",
            hover_data={
                "date": True,
                metric_col: False,
                "display_val": True
            }
        )
        
        # 优化图表交互：鼠标悬浮显示中文单位
        fig.update_traces(hovertemplate="<b>%{hovertext}</b><br>日期: %{x}<br>数值: %{customdata[0]}")
        fig.update_layout(
            hovermode="x unified",
            xaxis_title="日期",
            yaxis_title=selected_metric_label,
            legend_title="标的名称"
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- 图表 2: 指标分布表 ---
        st.divider()
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("📋 详细数据明细")
            # 格式化表格显示
            table_df = plot_df.copy()
            if 'amount' in table_df.columns:
                table_df['成交额'] = table_df['amount'].apply(lambda x: format_unit(x, 'amount'))
            
            display_cols = ['date', 'name', 'pct_chg', '成交额', 'turnover_rate', 'vol_ratio']
            st.dataframe(table_df[display_cols].sort_values('date', ascending=False), use_container_width=True, height=400)
            
        with col2:
            st.subheader("💡 统计概览")
            # 显示选中指标的平均值对比
            avg_stats = plot_df.groupby('name')[metric_col].mean().reset_index()
            fig_bar = px.bar(avg_stats, x='name', y=metric_col, color='name', title="周期内均值对比")
            st.plotly_chart(fig_bar, use_container_width=True)

# --- 页脚 ---
st.caption(f"系统环境: Python 3.10 | 管理工具: uv | 最后刷新: {datetime.now().strftime('%H:%M:%S')}")
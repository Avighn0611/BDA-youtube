"""Read-only interface for Spark-produced historical YouTube statistics."""
import json
from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from cluster import status

st.set_page_config(page_title='YouTube Pulse', page_icon='▶', layout='wide')
st.markdown('''<style>
.stApp {background:#0d111a; color:#eef2fa;}
[data-testid="stSidebar"] {background:#141b28;}
[data-testid="stMetric"] {background:#182131;border:1px solid #2a3547;border-radius:12px;padding:18px;}
h1,h2,h3 {letter-spacing:-.035em;} .eyebrow{color:#ff6772;letter-spacing:.16em;font-size:12px;font-weight:700;}
.stMainBlockContainer{padding-top:2.3rem;} a{color:#75d9ca;}
</style>''', unsafe_allow_html=True)
st.markdown('<div class="eyebrow">DISTRIBUTED DATA ANALYTICS / YOUTUBE</div>', unsafe_allow_html=True)
st.title('YouTube Pulse')
st.write('Explore the videos, categories, and engagement behind the trending list.')

ARTIFACTS = ROOT / 'artifacts'
if not (ARTIFACTS / 'snapshots.jsonl').exists():
    st.info('Run the Spark pipeline to generate dashboard data.')
    st.code('.venv/bin/python scripts/cluster.py start\n.venv/bin/python scripts/run_pipeline.py')
    st.stop()


@st.cache_data
def load_data(modified):
    frame = pd.read_json(ARTIFACTS / 'snapshots.jsonl', lines=True, convert_dates=False)
    frame['trending_date'] = pd.to_datetime(frame['trending_date'])
    return frame


def compact(value):
    for scale, suffix in [(1e9, 'B'), (1e6, 'M'), (1e3, 'K')]:
        if abs(value) >= scale:
            return f'{value/scale:,.2f}{suffix}'
    return f'{value:,.0f}'


def chart(figure):
    figure.update_layout(template='plotly_dark', paper_bgcolor='rgba(0,0,0,0)',
                         plot_bgcolor='rgba(0,0,0,0)', font_color='#dce4f2',
                         margin=dict(l=12, r=12, t=30, b=12), colorway=['#ff6772', '#75d9ca', '#a999ff'])
    st.plotly_chart(figure, use_container_width=True)


df = load_data((ARTIFACTS / 'snapshots.jsonl').stat().st_mtime_ns)
run = json.loads((ARTIFACTS / 'run.json').read_text())
with st.sidebar:
    st.markdown('### Explore the dataset')
    st.caption(f"{run['country']} · historical trending videos")
    dates = st.date_input('Trending dates', (df.trending_date.min().date(), df.trending_date.max().date()),
                          min_value=df.trending_date.min().date(), max_value=df.trending_date.max().date())
    selected = st.multiselect('Categories', sorted(df.category.unique()))
    search = st.text_input('Search video or channel', placeholder='e.g. music, cricket…')
    min_views = st.number_input('Minimum latest observed views', min_value=0, value=0, step=10000)
    st.divider()
    st.caption('Hadoop HDFS → PySpark → dashboard')
    st.caption('One Mac · two logical workers')
    st.link_button('Dataset source ↗', 'https://www.kaggle.com/datasets/datasnaek/youtube-new')

if len(dates) != 2:
    st.info('Choose both the start and end date.')
    st.stop()
filtered = df[df.trending_date.between(pd.Timestamp(dates[0]), pd.Timestamp(dates[1]))]
if selected:
    filtered = filtered[filtered.category.isin(selected)]
if search:
    filtered = filtered[filtered.title.str.contains(search, case=False, regex=False, na=False) |
                        filtered.channel_title.str.contains(search, case=False, regex=False, na=False)]
latest = filtered.sort_values(['trending_date', 'views']).drop_duplicates(['country', 'video_id'], keep='last')
latest = latest[latest.views >= min_views]
filtered = filtered[filtered.video_id.isin(latest.video_id)]
if latest.empty:
    st.warning('No videos match these filters. Try a wider date range or fewer restrictions.')
    st.stop()

st.caption(f"{dates[0]:%d %b %Y} — {dates[1]:%d %b %Y}  /  {len(filtered):,} daily observations  /  Data processed by Spark")
metrics = st.columns(4)
metrics[0].metric('Unique videos', f'{len(latest):,}')
metrics[1].metric('Latest observed views', compact(latest.views.sum()))
metrics[2].metric('Latest observed likes', compact(latest.likes.sum()))
engagement = latest.engagement_pct.median()
metrics[3].metric('Median engagement', f'{engagement:.2f}%' if pd.notna(engagement) else 'N/A')
st.caption('Totals use one latest observation per video within your date range. Views are cumulative, not views gained during the period.')

overview, trends, videos, cluster = st.tabs(['Category insights', 'Trending activity', 'Video explorer', 'Cluster & methodology'])
with overview:
    summary = latest.groupby('category', as_index=False).agg(videos=('video_id', 'count'), views=('views', 'sum'),
                   engagement=('engagement_pct', 'median'))
    left, right = st.columns(2)
    with left:
        st.subheader('Where the views concentrate')
        chart(px.bar(summary.sort_values('views'), x='views', y='category', orientation='h',
                     labels={'views': 'Latest observed views', 'category': ''}, color_discrete_sequence=['#ff6772']))
    with right:
        st.subheader('Which categories engage viewers')
        chart(px.bar(summary.sort_values('engagement'), x='engagement', y='category', orientation='h',
                     labels={'engagement': 'Median engagement (%)', 'category': ''}, color_discrete_sequence=['#75d9ca']))
    best = summary.loc[summary.views.idxmax()]
    st.info(f"In this selection, {best['category']} leads by observed views ({compact(best['views'])}) across {best['videos']:,} videos.")
    st.caption('Engagement = 100 × (likes + comments) / views. Zero-view and ratings-disabled observations have no engagement value.')
with trends:
    daily = filtered.groupby('trending_date', as_index=False).agg(videos=('video_id', 'nunique'), engagement=('engagement_pct', 'median'))
    st.subheader('Daily trending-list activity')
    chart(px.line(daily, x='trending_date', y='videos', labels={'trending_date': '', 'videos': 'Unique trending videos'}, markers=False))
    st.subheader('Engagement through the observation period')
    chart(px.line(daily, x='trending_date', y='engagement', labels={'trending_date': '', 'engagement': 'Median engagement (%)'}, color_discrete_sequence=['#75d9ca']))
    st.caption('Each date describes that day’s observed trending sample. Changes do not measure all YouTube activity or prove causation.')
with videos:
    st.subheader('The videos behind the numbers')
    sort = st.selectbox('Rank by', ['views', 'likes', 'engagement_pct', 'comment_count'])
    table = latest.sort_values(sort, ascending=False).copy()
    table['video_url'] = 'https://www.youtube.com/watch?v=' + table.video_id
    columns = ['title', 'channel_title', 'category', 'views', 'likes', 'comment_count', 'engagement_pct', 'trending_date', 'video_url']
    st.dataframe(table[columns], hide_index=True, use_container_width=True,
                 column_config={'video_url': st.column_config.LinkColumn('Video', display_text='Watch ↗'),
                                'engagement_pct': st.column_config.NumberColumn('Engagement %', format='%.2f')})
    # Escape spreadsheet formula prefixes in exported untrusted dataset text.
    exported = table[columns].copy()
    for col in ['title', 'channel_title', 'category']:
        exported[col] = exported[col].map(lambda v: "'" + v if isinstance(v, str) and v.startswith(('=', '+', '-', '@', '\t', '\r')) else v)
    st.download_button('Download filtered videos · CSV', exported.to_csv(index=False).encode('utf-8'), 'youtube-pulse.csv', 'text/csv')
with cluster:
    st.subheader('Real services. One physical host.')
    st.write('This demonstration uses two HDFS DataNodes and two Spark workers on this Mac. It has not been verified across three physical PCs.')
    if st.button('Refresh cluster status'):
        st.rerun()
    live = status()
    a, b = st.columns(2)
    a.metric('Live HDFS DataNodes', live['hdfs_live_datanodes'])
    b.metric('Live Spark workers', live['spark_alive_workers'])
    if live['errors']:
        st.caption('Cluster is offline or unreachable. Saved analytical results remain available.')
    st.markdown('[HDFS dashboard](http://127.0.0.1:9870) · [Spark master](http://127.0.0.1:8080)')
    st.write(f"Last successful application: `{run['application_id']}`")
    evidence_path = ARTIFACTS / 'execution-evidence.json'
    if evidence_path.exists():
        evidence = json.loads(evidence_path.read_text())
        if evidence['application_id'] == run['application_id']:
            st.write('Completed Spark tasks by executor (from the saved event log):')
            st.json(evidence['completed_tasks_per_executor'])
    st.write(f"Spark read {run['input_rows']:,} rows; retained {run['snapshot_rows']:,} snapshots and {run['unique_videos']:,} unique videos.")
    st.caption('Original CSV → HDFS → Spark cleaning and aggregation → HDFS JSON/Parquet → local dashboard export. Sidebar filters summarize those cleaned Spark outputs.')
    with st.expander('Run details and data limitations'):
        st.json(run)

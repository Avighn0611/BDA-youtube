"""Exercise real Streamlit interactions against the exported Spark dataset."""
from pathlib import Path
import pytest
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(not (ROOT / 'artifacts/snapshots.jsonl').exists(), reason='Run the Spark pipeline first')


def test_dashboard_load_filters_empty_state_and_ranking():
    app = AppTest.from_file(str(ROOT / 'dashboard/app.py'), default_timeout=30).run()
    assert not app.exception
    assert len(app.metric) >= 4
    original = int(app.metric[0].value.replace(',', ''))
    assert original > 0
    app.multiselect[0].set_value(['Music']).run()
    assert not app.exception
    assert 0 < int(app.metric[0].value.replace(',', '')) < original
    app.selectbox[0].select('likes').run()
    assert not app.exception
    app.text_input[0].set_value('NONEXISTENT-VIDEO-XYZ-987654321').run()
    assert not app.exception
    assert any('No videos match' in item.value for item in app.warning)
    app.text_input[0].set_value('').run()
    assert not app.exception

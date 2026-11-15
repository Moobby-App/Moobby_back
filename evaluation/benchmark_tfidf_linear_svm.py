"""Prepare and validate the frozen-v2 benchmark; never train or evaluate a model.

Run: MPLCONFIGDIR=/tmp/moobby-mpl .venv/bin/python -B \
    evaluation/benchmark_tfidf_linear_svm.py
Outputs go to a new timestamped directory. Importing this module performs no I/O.
"""

import argparse
import hashlib
import importlib.metadata
import json
import unicodedata
from datetime import datetime, timezone
from itertools import product
from pathlib import Path

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.svm import LinearSVC

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'evaluation/krl_tweets_clean.csv'
FROZEN = ROOT / 'evaluation/private/leakage_audit/final_split_v2_20261115T143218620874Z'
PARTITIONS = ('train', 'validation', 'test')
CLASSES = ('not_disruption', 'light', 'moderate', 'severe')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def normalize_text(text):
    """Exact historical notebook cell 11 normalization, without learned state."""
    return ' '.join(unicodedata.normalize('NFKC', text).casefold().split())


def load_data(source=SOURCE, frozen=FROZEN):
    """Replay historical selection with explicit accounting, then join by ID.

    No new deduplication policy: keep-first on normalized text reproduces the
    historical modeling population, verified against the frozen audit snapshot.
    Any membership, text, label or checksum discrepancy aborts preparation.
    """
    manifest = json.loads((frozen / 'split_manifest.json').read_text())
    require(manifest['status'] == 'FROZEN', 'Split must be FROZEN')
    assignment_path = frozen / manifest['assignment_file']
    require(sha256(assignment_path) == manifest['assignment_sha256'], 'Assignment hash mismatch')
    for line in (frozen / 'SHA256SUMS').read_text().splitlines():
        digest, name = line.split('  ', 1)
        require(sha256(frozen / name) == digest, f'Frozen checksum mismatch: {name}')
    raw = pd.read_csv(source, dtype=str, keep_default_na=False, encoding='utf-8-sig')
    require(raw['tweet_id'].is_unique and raw['tweet_id'].ne('').all(), 'Invalid raw IDs')
    raw['model_text'] = raw['full_text'].map(normalize_text)
    variations = raw.groupby('model_text')[['disruption_label', 'severity']].nunique()
    require(not variations.gt(1).any().any(), 'Conflicting labels within historical text groups')
    model = raw.loc[~raw['model_text'].duplicated(keep='first')].copy()
    assignments = pd.read_csv(assignment_path, dtype=str, keep_default_na=False)
    require(len(model) == len(assignments) == 1311, 'Expected exactly 1,311 records')
    require(assignments['tweet_id'].is_unique, 'Duplicate assignment IDs')
    require(set(model['tweet_id']) == set(assignments['tweet_id']), 'Missing or extra IDs')
    snapshot_items = [(ROOT / p, h) for p, h in manifest['source_audit_input_hashes'].items()
                      if 'existing_audit_state_' in p]
    require(len(snapshot_items) == 1, 'Expected one historical modeling snapshot')
    snapshot_path, digest = snapshot_items[0]
    require(sha256(snapshot_path) == digest, 'Snapshot hash mismatch')
    historical = pd.DataFrame(json.loads(snapshot_path.read_text())['df_model']['data'])
    observed = model.set_index('tweet_id').sort_index()
    expected = historical.set_index('tweet_id').sort_index()
    require(observed.index.equals(expected.index), 'Historical modeling ID mismatch')
    for column in ['full_text', 'model_text', 'disruption_label', 'severity']:
        require(observed[column].equals(expected[column].fillna('')), f'Historical mismatch: {column}')
    joined = model.merge(assignments, on='tweet_id', how='outer', validate='one_to_one', indicator=True)
    require(joined['_merge'].eq('both').all(), 'Incomplete join')
    joined = joined.drop(columns='_merge')
    require(joined['partition'].isin(PARTITIONS).all(), 'Invalid partition')
    require(joined['model_text'].str.strip().ne('').all(), 'Empty model input')
    require(joined['disruption_label'].eq(joined['original_relevance']).all(), 'Relevance mismatch')
    require(joined['severity'].eq(joined['original_severity']).all(), 'Severity mismatch')
    require(joined['disruption_label'].isin(['disruption', 'not_disruption']).all(), 'Invalid relevance')
    disruption = joined['disruption_label'].eq('disruption')
    require(joined.loc[disruption, 'severity'].isin(CLASSES[1:]).all(), 'Invalid severity')
    require(joined.loc[~disruption, 'severity'].eq('').all(), 'Non-disruption has severity')
    joined['audit_class'] = joined['severity'].where(disruption, 'not_disruption')
    distribution = pd.crosstab(joined['partition'], joined['audit_class']).reindex(
        index=PARTITIONS, columns=CLASSES, fill_value=0)
    require(distribution.to_dict(orient='index') == manifest['validation']['class_distribution'],
            'Class counts differ from frozen manifest')
    identified = joined.loc[joined['event_id'].ne('')]
    require(identified.groupby('event_id')['partition'].nunique().le(1).all(), 'Identified event leakage')
    require(joined.groupby('original_similarity_group')['partition'].nunique().le(1).all(), 'Similarity group leakage')
    split = {p: joined.loc[joined['partition'].eq(p)].copy() for p in PARTITIONS}
    summary = {'status': 'PASS', 'raw_records': len(raw), 'historical_text_duplicates_excluded': len(raw)-len(model),
               'model_records': len(joined), 'unique_tweet_ids': joined['tweet_id'].nunique(),
               'missing_ids': 0, 'extra_ids': 0, 'class_distribution': distribution.to_dict(orient='index'),
               'partition_counts': {p: len(d) for p, d in split.items()},
               'identified_events_crossing': 0, 'similarity_groups_crossing': 0,
               'source': str(source), 'source_sha256': sha256(source),
               'assignment': str(assignment_path), 'assignment_sha256': sha256(assignment_path),
               'manifest_sha256': sha256(frozen / 'split_manifest.json'),
               'snapshot_sha256': digest, 'limitations': manifest['limitations'],
               'training_executed': False, 'test_evaluated': False}
    return split, distribution, summary


def experiment_configs():
    """12 predeclared configurations per task; selection metric validation macro-F1."""
    return [dict(ngram_range=n, C=c, class_weight=w)
            for n, c, w in product([(1, 1), (1, 2)], [0.1, 1.0, 10.0], [None, 'balanced'])]


def build_pipeline(config):
    """Unfitted estimator factory. Text must already be historical model_text.

    Future training must call pipeline.fit only on split['train']. Relevance
    uses all train posts; severity uses true-disruption train posts only.
    Fit each task's vectorizer independently. Never reuse audit TF-IDF matrices.
    """
    return Pipeline([
        ('tfidf', TfidfVectorizer(lowercase=False, ngram_range=config['ngram_range'],
                                 sublinear_tf=True, min_df=1, max_df=1.0, norm='l2')),
        ('svm', LinearSVC(C=config['C'], class_weight=config['class_weight'],
                          random_state=42, max_iter=10000, dual='auto')),
    ])


def make_visuals(output, distribution, summary):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    def flow(name, title, labels):
        fig, ax = plt.subplots(figsize=(14, 3.2))
        ax.set(xlim=(0, 1), ylim=(0, 1)); ax.axis('off')
        for i, label in enumerate(labels):
            x = (i + .5) / len(labels)
            ax.text(x, .5, label, ha='center', va='center', fontsize=10,
                    bbox=dict(boxstyle='round,pad=.6', facecolor='#e6f0fa', edgecolor='#3973ac'))
            if i:
                ax.annotate('', xy=(x-.085,.5), xytext=(x-1/len(labels)+.085,.5),
                            arrowprops=dict(arrowstyle='->',color='#3973ac'))
        ax.set_title(title, pad=16); fig.tight_layout()
        fig.savefig(output/name, dpi=170); plt.close(fig)
    flow('dataset_flow.png', 'Historical data selection → Frozen split → Benchmark readiness', [
        f"Raw source\n{summary['raw_records']:,} posts", 'Historical NFKC\ncasefold + whitespace\nkeep-first text selection',
        f"Modeling population\n{summary['model_records']:,} posts\nID/text checked against snapshot",
        'Final split v2\n1:1 join on tweet_id\n' + ' / '.join(str(summary['partition_counts'][p]) for p in PARTITIONS), 'Two unfitted pipelines\nRelevance + Severity'])
    ax=distribution.plot.bar(figsize=(10,5), rot=0, color=['#3973ac','#63a67a','#eea036','#bc4f62'])
    ax.set(title='Frozen v2 — Class distribution by partition', xlabel='Partition', ylabel='Number of posts')
    for container in ax.containers: ax.bar_label(container, padding=3, fontsize=9)
    ax.set_ylim(0, max(distribution.max())*1.2); ax.legend(title='Class')
    ax.figure.tight_layout(); ax.figure.savefig(output/'class_distribution.png', dpi=170); plt.close(ax.figure)
    flow('evaluation_flow.png', 'Planned evaluation protocol — NOT executed', [
        'Train only\nFit new TF-IDF + SVM\nSeparate pipeline per task',
        'Validation\nTransform + predict\nSelect by macro-F1',
        'Lock configuration\nKeep selected train-fit model\nNo train+validation refit',
        'Final test once\nOnly after explicit approval\nReport relevance + severity'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=None)
    args=parser.parse_args()
    split, distribution, summary=load_data()
    for config in experiment_configs():
        pipeline=build_pipeline(config)
        require(not hasattr(pipeline['tfidf'], 'vocabulary_') and not hasattr(pipeline['svm'], 'classes_'),
                'Estimator unexpectedly fitted')
    output=args.output or ROOT/'evaluation/private/benchmark_readiness'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    output.mkdir(parents=True, exist_ok=False)
    summary['library_versions']={p:importlib.metadata.version(p) for p in ['pandas','numpy','scipy','scikit-learn','matplotlib']}
    summary['python_version']=__import__('platform').python_version()
    summary['prepared_configs_per_task']=len(experiment_configs())
    summary['tasks']={'relevance':'All rows; disruption/not_disruption', 'severity':'True disruption only; light/moderate/severe'}
    summary['selection_protocol']='Validation macro-F1 per task; tie: lower C then unigrams then no class weight. Test untouched.'
    (output/'readiness_report.json').write_text(json.dumps(summary,indent=2)+'\n')
    (output/'experiment_configs.json').write_text(json.dumps(experiment_configs(),indent=2)+'\n')
    make_visuals(output, distribution, summary)
    print(json.dumps({'output_directory':str(output),**summary},indent=2))


if __name__ == '__main__':
    main()

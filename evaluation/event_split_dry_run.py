"""Candidate allocation experiments only. No input partitions are overwritten.

Allocation blocks are constraints, not new event identities. Unknown relations
remain unknown even when an experiment colocates their endpoints.
"""
import argparse
from collections import Counter
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import platform
from uuid import uuid4

import numpy as np
import pandas as pd
import scipy
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.sparse import coo_matrix

from event_leakage_screening import load_snapshot, frame_digest

PARTS = ("train", "validation", "test")
CLASSES = ("not_disruption", "light", "moderate", "severe")
TIE_SEED = 42
VERSION = "event-split-dry-run-v1"


@dataclass(frozen=True)
class Inputs:
    manifest: pd.DataFrame
    relations: tuple
    templates: pd.DataFrame
    original_manifest: pd.DataFrame
    hashes: dict


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_inputs(directory, snapshot_path):
    directory = Path(directory)
    names = ("full_event_identity_manifest.csv", "relationship_evidence.jsonl",
             "template_overlap_report.csv", "event_leakage_report.csv", "audit_metrics.json")
    hashes = {str(directory / name): file_hash(directory / name) for name in names}
    hashes[str(snapshot_path)] = file_hash(snapshot_path)
    manifest = pd.read_csv(directory / names[0], dtype=str, keep_default_na=False)
    templates = pd.read_csv(directory / names[2], dtype=str, keep_default_na=False)
    relations = tuple(json.loads(line) for line in (directory / names[1]).read_text().splitlines())
    state = load_snapshot(snapshot_path)
    assert manifest.tweet_id.tolist() == state.df_model.tweet_id.tolist()
    assert len(manifest) == 1311 and manifest.tweet_id.is_unique
    for original, source in (("original_partition", "partition"), ("original_relevance", "disruption_label"),
                             ("original_severity", "severity"), ("original_similarity_group", "similarity_group")):
        assert manifest[original].tolist() == state.df_model[source].astype(str).tolist()
    indexed = manifest.set_index("tweet_id")
    for row in state.audit_manifest.to_dict("records"):
        assert indexed.loc[row["tweet_id"], "event_id"] == row["event_id"]
        assert indexed.loc[row["tweet_id"], "event_identity_confidence"] == "prior_human_verified"
    ids = set(manifest.tweet_id)
    for row in relations:
        assert row["tweet_id_a"] in ids and row["tweet_id_b"] in ids
    original_overlaps = manifest.loc[manifest.event_id.ne("")].groupby("event_id").original_partition.nunique()
    assert len(original_overlaps[original_overlaps > 1]) == 8
    return Inputs(manifest, relations, templates, state.audit_manifest.copy(deep=True), hashes)


def unresolved_edges(inputs, tier):
    rows = [r for r in inputs.relations if r["relation"] == "UNRESOLVED"]
    if tier == "none": return []
    if tier == "all": return rows
    assert tier == "specific"
    result = []
    for r in rows:
        e = r["evidence"]
        if e["hours_apart"] is None or e["hours_apart"] > 24: continue
        if ((e["shared_train_numbers"] and e["shared_incident_locations"])
                or (len(e["shared_incident_locations"]) >= 2 and e["shared_causes"])):
            result.append(r)
    return result


def allocation_blocks(inputs, tier):
    """Construct dry-run allocation units; never update event_id or old groups."""
    m = inputs.manifest
    parent = list(range(len(m)))
    positions = {tid: i for i, tid in enumerate(m.tweet_id)}
    def root(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]; i = parent[i]
        return i
    def union(a,b): parent[root(b)] = root(a)
    for column in ("original_similarity_group", "event_id"):
        for key, frame in m.groupby(column, sort=True):
            if column == "event_id" and key == "": continue
            indices = frame.index.tolist()
            for i in indices[1:]: union(indices[0], i)
    for r in unresolved_edges(inputs, tier):
        union(positions[r["tweet_id_a"]], positions[r["tweet_id_b"]])
    blocks = {}
    for i in range(len(m)):blocks.setdefault(root(i), []).append(i)
    return sorted(blocks.values(), key=lambda b: min(m.iloc[b].tweet_id))


def targets(inputs):
    m=inputs.manifest
    classes=np.where(m.original_relevance.eq("not_disruption"),"not_disruption",m.original_severity)
    expected=np.array([[sum((m.original_partition.to_numpy()==p)&(classes==c)) for c in CLASSES] for p in PARTS])
    return classes, expected


def majority_candidate(inputs, blocks):
    assignment=np.empty(len(inputs.manifest),dtype=object)
    for members in blocks:
        counts=Counter(inputs.manifest.iloc[members].original_partition)
        part=max(PARTS,key=lambda p:counts[p])
        assignment[members]=part
    return assignment, {"solver":"independent_block_majority", "optimal_record_moves":True,
                        "tie_break":"train, validation, test"}


def balanced_candidate(inputs, blocks, require_severe_units=False):
    """Lexicographic movement minimum then seeded stable tie cost, exact classes."""
    m=inputs.manifest
    classes,expected=targets(inputs)
    n=len(blocks); costs=np.zeros(n*3)
    rows=[]; cols=[]; values=[]
    max_tie=100003
    primary_weight=n*max_tie+1
    for b,members in enumerate(blocks):
        counts=Counter(m.iloc[members].original_partition)
        class_counts=Counter(classes[members])
        key=min(m.iloc[members].tweet_id)
        for p,part in enumerate(PARTS):
            col=b*3+p
            tie=int(hashlib.sha256(f"{TIE_SEED}|{key}|{part}".encode()).hexdigest()[:12],16)%max_tie
            costs[col]=(len(members)-counts[part])*primary_weight+tie
            rows.append(b);cols.append(col);values.append(1)
            for c,label in enumerate(CLASSES):
                if class_counts[label]:
                    rows.append(n+p*4+c);cols.append(col);values.append(class_counts[label])
            if require_severe_units:
                block=m.iloc[members]
                count=block.loc[block.original_severity.eq('severe') & block.event_id.ne(''),'event_id'].nunique()
                if count:rows.append(n+12+p);cols.append(col);values.append(count)
    rhs=np.concatenate([np.ones(n),expected.flatten(),np.ones(3) if require_severe_units else []])
    upper=rhs.copy()
    if require_severe_units:upper[-3:]=np.inf
    matrix=coo_matrix((values,(rows,cols)),shape=(len(rhs),n*3)).tocsc()
    result=milp(costs,integrality=np.ones(n*3),bounds=Bounds(0,1),
                constraints=LinearConstraint(matrix,rhs,upper),
                options={"time_limit":60,"mip_rel_gap":0.0,"presolve":True})
    info={"solver":"scipy.optimize.milp/HiGHS", "status":int(result.status),
          "message":str(result.message),"primary_weight":primary_weight,
          "tie_seed":TIE_SEED,"exact_original_class_totals":True,
          "minimum_identified_severe_units_per_partition":int(require_severe_units),
          "optimal_record_moves":result.status==0}
    if result.status != 0:return None,info
    chosen=np.rint(result.x).astype(int).reshape(n,3)
    assert np.all(chosen.sum(axis=1)==1)
    assignment=np.empty(len(m),dtype=object)
    for members,selection in zip(blocks,chosen):assignment[members]=PARTS[int(np.argmax(selection))]
    for p,part in enumerate(PARTS):
        for c,label in enumerate(CLASSES):assert sum((assignment==part)&(classes==label))==expected[p,c]
    return assignment,info


def candidate_metrics(inputs, name, proposed, tier, info, baseline=False):
    m=inputs.manifest
    assert len(proposed)==1311 and set(proposed)<=set(PARTS)
    # Local scratch frame, distinct proposed column. Baseline never reassigned.
    scratch=m.assign(proposed_partition=proposed)
    event_cross=scratch.loc[scratch.event_id.ne("")].groupby("event_id").proposed_partition.nunique()
    group_cross=scratch.groupby("original_similarity_group").proposed_partition.nunique()
    if not baseline:
        assert event_cross.eq(1).all(),"An identified event is still split"
        assert group_cross.eq(1).all(),"An original near-duplicate group is still split"
    id_parts=dict(zip(m.tweet_id,proposed))
    unresolved=[r for r in inputs.relations if r["relation"]=="UNRESOLVED"
                and id_parts[r["tweet_id_a"]]!=id_parts[r["tweet_id_b"]]]
    original_parts=dict(zip(m.tweet_id,m.original_partition))
    newly_crossing=[r for r in unresolved if original_parts[r["tweet_id_a"]]==original_parts[r["tweet_id_b"]]]
    original_unresolved_crossing=[r for r in inputs.relations if r['relation']=='UNRESOLVED'
                                 and original_parts[r['tweet_id_a']]!=original_parts[r['tweet_id_b']]]
    severity_by_id=dict(zip(m.tweet_id,m.original_severity))
    selected=unresolved_edges(inputs,tier)
    if not baseline:assert all(id_parts[r['tweet_id_a']]==id_parts[r['tweet_id_b']] for r in selected)
    blocks=(list(m.groupby('original_similarity_group').indices.values()) if baseline else allocation_blocks(inputs,tier))
    template_cross=sum(len({id_parts[t] for t in json.loads(r['tweet_ids'])})>1 for r in inputs.templates.to_dict('records'))
    metrics={"candidate":name,"sensitivity_tier":tier,
             "moved_records":int(sum(proposed!=m.original_partition.to_numpy())),
             "identified_events_crossing":int(event_cross.gt(1).sum()),
             "original_similarity_groups_crossing":int(group_cross.gt(1).sum()),
             "unresolved_crossing_edges":len(unresolved),
             "unresolved_crossing_posts":len({r[k] for r in unresolved for k in ('tweet_id_a','tweet_id_b')}),
             "newly_crossing_unresolved_edges":len(newly_crossing),
             "previously_crossing_unresolved_edges_colocated":len(original_unresolved_crossing)-(len(unresolved)-len(newly_crossing)),
             "severe_unresolved_crossing_edges":sum(
                 severity_by_id[r['tweet_id_a']]=='severe' or severity_by_id[r['tweet_id_b']]=='severe'
                 for r in unresolved),
             "crossing_reported_template_groups":template_cross,
             "allocation_blocks":len(blocks),
             "max_allocation_block":max(map(len,blocks)),
             "unresolved_edges_constrained":len(selected),**info}
    for part in PARTS:
        subset=scratch.loc[scratch.proposed_partition.eq(part)]
        metrics[part+'_posts']=len(subset)
        metrics[part+'_identified_event_units']=subset.loc[subset.event_id.ne(''),'event_id'].nunique()
        metrics[part+'_prior_human_event_units']=subset.loc[subset.event_identity_confidence.eq('prior_human_verified'),'event_id'].nunique()
        metrics[part+'_unresolved_posts']=int(subset.event_identity_status.eq('UNRESOLVED').sum())
        metrics[part+'_commuterline_pct']=round(subset.source_account.eq('@CommuterLine').mean()*100,2)
        for label in CLASSES:
            mask=subset.original_relevance.eq(label) if label=='not_disruption' else subset.original_severity.eq(label)
            metrics[part+'_'+label]=int(mask.sum())
        severe=subset.loc[subset.original_severity.eq('severe')]
        metrics[part+'_identified_severe_event_units']=severe.loc[severe.event_id.ne(''),'event_id'].nunique()
    return metrics


def propose(inputs):
    baseline_hash=frame_digest(inputs.manifest)
    candidates={};rows=[]
    baseline=inputs.manifest.original_partition.to_numpy().copy()
    rows.append(candidate_metrics(inputs,'C0_ORIGINAL',baseline,'none',{},True))
    configurations=(('C1_MINIMUM_MOVEMENT','none',False,False),
                    ('C2_EXACT_CLASS_BALANCE','none',True,False),
                    ('C3_SPECIFIC_RISK_AND_SEVERE_COVERAGE','specific',True,True),
                    ('C4_ALL_UNRESOLVED_ENVELOPE','all',False,False),
                    ('C5_BALANCE_AND_SEVERE_COVERAGE','none',True,True))
    for name,tier,balanced,severe_coverage in configurations:
        blocks=allocation_blocks(inputs,tier)
        proposed,info=(balanced_candidate(inputs,blocks,severe_coverage) if balanced else majority_candidate(inputs,blocks))
        if proposed is None:
            rows.append({'candidate':name,'feasible':False,'sensitivity_tier':tier,**info})
            continue
        rows.append({'feasible':True,**candidate_metrics(inputs,name,proposed,tier,info)})
        candidates[name]=proposed
    assert baseline_hash==frame_digest(inputs.manifest)
    for p,h in inputs.hashes.items():assert file_hash(p)==h
    return candidates,pd.DataFrame(rows)


def write_proposal(inputs,candidates,comparison,directory):
    output=Path(directory)/('split_dry_run_'+uuid4().hex)
    output.mkdir(parents=True,exist_ok=False)
    comparison.to_csv(output/'candidate_comparison.csv',index=False,mode='x')
    assignments=inputs.manifest[['tweet_id','original_partition','original_relevance','original_severity',
                                'original_similarity_group','event_id','event_identity_confidence']].copy()
    for name,values in candidates.items():assignments['proposed_'+name]=values
    assignments.to_csv(output/'DRY_RUN_candidate_assignments.csv',index=False,mode='x')
    provenance={'version':VERSION,'tie_seed':TIE_SEED,'python':platform.python_version(),
                'numpy':np.__version__,'pandas':pd.__version__,'scipy':scipy.__version__,
                'input_sha256':inputs.hashes,'code_sha256':file_hash(__file__),
                'original_human_event_posts':43,'identified_event_units':13,
                'original_data_labels_partitions_and_event_provenance_unchanged':True}
    (output/'verification.json').write_text(json.dumps(provenance,indent=2))
    lines=['# DRY-RUN event-aware allocation comparison — NOT ACTIVATED','',
           'No dataset, original split, event identity, label, notebook or training artifact was replaced.',
           'Candidate columns are hypothetical allocations only. They are never loaded automatically by retrain.ipynb.','',
           '## Constraints and candidate definitions','',
           '- All candidates retain 1311 records exactly once and preserve original labels, partitions and audit provenance as separate baseline columns.',
           '- All 13 identified event units (six human-origin, six automated hypotheses, one analyst-adjudicated hypothesis) are indivisible allocation constraints. The original 1231 similarity groups remain indivisible too.',
           '- An allocation block can include several event IDs; colocation does not merge their identities or promote a hypothesis to a human confirmation.',
           '- C1: independent block majority, mathematically minimum record movements for these hard constraints; fixed train/validation/test tie order.',
           '- C2: minimum movements subject to EXACT original per-partition counts for non-disruption/light/moderate/severe. Thus original 786/262/263 sizes and 39/13/14 severe counts are maintained.',
           '- C3: C2 plus specific unresolved-risk links: publication separation <=24h and either shared KA + incident location, or >=2 shared incident locations + shared cause. It additionally requires at least one identified severe event unit in each partition. These are only conservative allocation constraints, not event confirmations.',
           '- C4: upper-bound sensitivity envelope blocking EVERY recorded unresolved link, including links between distant publication episodes. Majority allocation is used without balance constraints. This deliberately overconservative scenario can colocate unrelated incidents and is not recommended as an identity model.',
           '- C5: C2 plus the same one-identified-severe-unit-per-partition guard as C3, without unresolved-link blocks. This isolates the additional movement cost of severe event coverage.',
           '- C2/C3 use integer optimization with movement cost dominating the sum of all stable SHA-256 tie costs (seed salt 42). Optimality is reported; infeasible/timeout cases have no fabricated assignment.',
           '- Stable ordering, fixed input and pinned library/solver versions support reproducibility. No model accuracy or test predictions enter the optimization.','',
           '## Actual comparison','',
           '| Candidate | Moves | Train/val/test | Event overlap | Unresolved crossing edges | Newly crossing edges | Max block |',
           '|---|---:|---|---:|---:|---:|---:|']
    for r in comparison.to_dict('records'):
        if pd.isna(r.get('moved_records')):
            lines.append(f"| {r['candidate']} | infeasible/no optimal solution | — | — | — | — | — |")
        else:
            lines.append(f"| {r['candidate']} | {int(r['moved_records'])} | {int(r['train_posts'])}/{int(r['validation_posts'])}/{int(r['test_posts'])} | {int(r['identified_events_crossing'])} | {int(r['unresolved_crossing_edges'])} | {int(r['newly_crossing_unresolved_edges'])} | {int(r['max_allocation_block'])} |")
    lines += ['', '## Identified event units and severe coverage', '',
              'These counts refer only to identified units among the 72 assigned posts, not the true total number of incidents. Original-split units can be counted in multiple partitions. Candidate unit totals do not duplicate units across partitions.', '',
              '| Candidate | Identified event units train/val/test | Severe posts train/val/test | Identified severe event units train/val/test |',
              '|---|---|---|---|']
    for r in comparison.to_dict('records'):
        if pd.isna(r.get('moved_records')):continue
        values=['/'.join(str(int(r[p+suffix])) for p in PARTS) for suffix in
                ('_identified_event_units','_severe','_identified_severe_event_units')]
        lines.append('| '+r['candidate']+' | '+' | '.join(values)+' |')
    lines += ['', '## Class counts', '', '| Candidate/partition | Non-disruption | Light | Moderate | Severe |', '|---|---:|---:|---:|---:|']
    for r in comparison.to_dict('records'):
        if pd.isna(r.get('moved_records')):continue
        for p in PARTS:lines.append('| '+r['candidate']+'/'+p+' | '+' | '.join(str(int(r[p+'_'+c])) for c in CLASSES)+' |')
    lines += ['', '## Recommendation: C3, conditional on explicit future approval', '',
              'C3 is the recommended provisional strategy among these candidates: retain exact baseline class counts, require identified severe-event representation in every partition, keep all identified events/original similarity groups intact, and colocate the selected time/detail-supported uncertainty links.',
              'C5 is the lower-movement fallback if provisional uncertainty blocks are considered too speculative. C1/C2 are not recommended for severe event evaluation because they leave no identified severe event unit in test. C4 is a stress-test envelope, not a recommended split: it puts every identified event in training and distorts class distributions.',
              'This recommendation is a research-design tradeoff, not a claim that C3 is statistically optimal or contamination-free. The priority is not selected using model test scores. A test with only one identified severe event unit still cannot demonstrate broad severe-event generalization.',
              'Do not activate C3 or any proposal until separately approved. Keep the baseline and complete audit provenance as immutable versions.',
              '', '## Uncertainty sensitivity analysis', '',
              '| Candidate | Unresolved edges constrained | Old crossing edges colocated | Newly crossing unresolved edges | All remaining crossing edges |',
              '|---|---:|---:|---:|---:|']
    for r in comparison.to_dict('records'):
        if pd.isna(r.get('moved_records')):continue
        lines.append('| '+r['candidate']+' | '+' | '.join(str(int(r[k])) for k in
            ('unresolved_edges_constrained','previously_crossing_unresolved_edges_colocated',
             'newly_crossing_unresolved_edges','unresolved_crossing_edges'))+' |')
    lines += ['', 'Specific uncertainty blocks use 24 hours plus location/cause/train evidence; this is a disclosed sensitivity assumption, not an incident-identity conclusion. All-link closure is the deliberately broad opposite extreme.',
              'C3/C5 differ only by those specific uncertainty blocks. C2/C5 isolate the effect of identified severe-unit coverage. C1/C2 isolate the cost of preserving exact class counts. C4 demonstrates that forcing every observed uncertainty link together can erase useful evaluation diversity.',
              'Edges are correlated hypotheses, not verified incidents or calibrated risk units. A reduction in their count must not be presented as a measured reduction in true leakage.',
              '', '## Interpretation and limits', '',
              'The original eight cross-partition event units are eliminated in every feasible proposal; all 13 identified units and original similarity-group separation are checked.',
              'Unresolved links are evaluated over ALL 1090 saved unresolved relationships, not only the original 597 crossing ones. Moves may create new crossing links even while fixing known events; those are counted explicitly.',
              'No unresolved relation is relabeled resolved by this procedure. Even C4 having zero recorded unresolved crossings would not establish absence of undiscovered leakage.',
              '28 template-overlap groups in the existing audit are rechecked under each proposal. This covers the reported template groups, not an exhaustive new template discovery.',
              'Class-count preservation does not ensure event/source diversity. There are only 13 identified units; the 39 severe training posts cannot be assumed to be 39 independent incidents.',
              'Source proportions and unresolved-post counts per partition are in candidate_comparison.csv. Source balance does not prove model fairness or unseen-source generalization.',
              'The baseline test has been examined for leakage only; do not select models, tune prediction thresholds or select these allocations using test performance.',
              'The DRY_RUN_candidate_assignments.csv file is evidence for comparison, not an activated split. Event-aware resplitting requires separate authorization.']
    lines += ['', '## Severe uncertainty and source sensitivity', '',
              '| Candidate | Severe-related unresolved crossing edges | Unresolved posts train/val/test | Reported template groups still crossing | CommuterLine percent train/val/test |',
              '|---|---:|---|---:|---|']
    for r in comparison.to_dict('records'):
        if pd.isna(r.get('moved_records')):continue
        unresolved='/'.join(str(int(r[p+'_unresolved_posts'])) for p in PARTS)
        sources='/'.join(str(r[p+'_commuterline_pct']) for p in PARTS)
        lines.append(f"| {r['candidate']} | {int(r['severe_unresolved_crossing_edges'])} | {unresolved} | {int(r['crossing_reported_template_groups'])} | {sources} |")
    lines += ['', 'C3 reduces the overall count of crossing unresolved edges but can increase severe-related unresolved edges and unresolved test posts compared with the baseline. This is a material tradeoff, not evidence that severe contamination was solved.',
              'None of these candidates establishes that all severe examples are independent incidents. Remaining uncertainty is preserved rather than converted into confirmed identities or leakage-free claims.']
    (output/'event_aware_dry_run_proposal.md').write_text('\n'.join(lines)+'\n')
    return output


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--audit-directory',required=True)
    parser.add_argument('--snapshot',required=True);parser.add_argument('--output-directory',required=True)
    args=parser.parse_args()
    inputs=load_inputs(args.audit_directory,args.snapshot)
    candidates,comparison=propose(inputs)
    output=write_proposal(inputs,candidates,comparison,args.output_directory)
    print(comparison[['candidate','moved_records','train_posts','validation_posts','test_posts',
                      'identified_events_crossing','unresolved_crossing_edges','newly_crossing_unresolved_edges',
                      'max_allocation_block']].to_string(index=False))
    print('OUTPUT',output)

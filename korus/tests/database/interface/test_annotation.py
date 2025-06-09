import pytest
from datetime import datetime


def test_generate_negatives(interfaces_with_taxonomy):
    file = interfaces_with_taxonomy["file"]
    job = interfaces_with_taxonomy["job"]
    annot = interfaces_with_taxonomy["annotation"]

    # add job with no target
    job.add({"taxonomy_id": 0})

    # add a couple of 10-s long audio files without timestamps
    frow = {
        "deployment_id": 0,
        "storage_id": 0,
        "sample_rate": 2000,
        "num_samples": 20000,
    }
    # add two files
    frow["filename"] = "file1.wav"
    file.add(frow)
    frow["filename"] = "file2.wav"
    file.add(frow)

    # add files to job
    job.add_file(0, 0, channel=0)
    job.add_file(0, 1, channel=0)

    # generate negatives
    annot.generate_negatives(0)

    # there should be two negatives, one for each file
    indices = annot.filter(negative=True).indices
    assert indices == [0, 1]

    # check the annotation data for these two negatives
    df = annot.get(
        indices,
        fields=["duration", "start", "excluded_label"],
        as_pandas=True,
        return_indices=True,
    )
    expected = """    duration  start excluded_label
id                                
0       10.0    0.0           None
1       10.0    0.0           None"""
    assert df.to_string() == expected

    # set the target for the job
    job.set(0, {"target": [("KW", "TC")]})

    # re-generate negatives
    annot.generate_negatives(0)

    # there should again be two negatives, AND excluded_label should now be same as job target
    indices = annot.reset_filter().filter(negative=True).indices
    assert indices == [0, 1]
    df = annot.get(
        indices,
        fields=["duration", "start", "excluded_label"],
        as_pandas=True,
        return_indices=True,
    )
    expected = """    duration  start excluded_label
id                                
0       10.0    0.0     [(KW, TC)]
1       10.0    0.0     [(KW, TC)]"""
    assert df.to_string() == expected

    # add an annotation, then re-generate negatives
    row = {
        "deployment_id": 0,
        "job_id": 0,
        "file_id": 0,
        "label": ("HW", "TC"),
        "duration": 2.0,
        "start": 4.0,
    }
    annot.add(row)
    annot.generate_negatives(0)

    # there should now be 3 negatives
    indices = annot.reset_filter().filter(negative=True).indices
    assert len(indices) == 3
    df = annot.get(
        indices,
        fields=["duration", "start", "label", "excluded_label"],
        as_pandas=True,
        return_indices=True,
    )
    expected = """    duration  start label excluded_label
id                                      
1        4.0    0.0  None     [(KW, TC)]
2        4.0    6.0  None     [(KW, TC)]
3       10.0    0.0  None     [(KW, TC)]"""
    assert df.to_string() == expected

    # filtering on exclude=(KW,*) returns the humpback call and the negatives
    indices = annot.reset_filter().filter(exclude=("KW", "*")).indices
    assert len(indices) == 4

    # negatives can be omitted by filtering on negative=False
    indices = annot.reset_filter().filter(exclude=("KW", "*"), negative=False).indices
    assert len(indices) == 1

    df = annot.get(
        indices=indices,
        fields=["duration", "start", "label", "excluded_label"],
        as_pandas=True,
        return_indices=True,
    )
    expected = """    duration  start     label excluded_label
id                                          
0        2.0    4.0  (HW, TC)           None"""
    assert df.to_string() == expected


def test_add_get_set_filter_data(interfaces_with_taxonomy):
    """Check that we can add data to, retrieve data from, and modify data in an AnnotationInterface instance"""

    job = interfaces_with_taxonomy["job"]
    tag = interfaces_with_taxonomy["tag"]
    annot = interfaces_with_taxonomy["annotation"]

    # add a job
    job.add({"taxonomy_id": 0})

    # attempt to add an annotation with invalid tags fails
    row = {
        "deployment_id": 0,
        "job_id": 0,
        "label": ("SRKW", "S01"),
        "start_utc": datetime(2022, 1, 1),
        "duration": 3.0,
        "start": 17.1,
        "tag": ["loud", "pretty"],
    }
    with pytest.raises(ValueError):
        annot.add(row)

    # add the tags
    tag.add({"name": "loud", "description": "a loud sound"})
    tag.add({"name": "pretty", "description": "a pretty sound"})
    tag.add({"name": "faint", "description": "a faint sound"})

    # try again
    annot.add(row)

    # add another annotation
    row = {
        "deployment_id": 0,
        "job_id": 0,
        "label": ("KW", "PC"),
        "start_utc": datetime(2023, 1, 1),
        "duration": 2.0,
        "start": 18.1,
        "tag": "faint",
        "granularity": "batch",
    }
    annot.add(row)

    # no condition
    idx = annot.reset_filter().filter().indices
    assert idx == [0, 1]

    # --- filter using condition ---
    idx = annot.reset_filter().filter({"label": ("SRKW", "*")}).indices
    assert idx == [0]

    idx = annot.reset_filter().filter({"granularity": "batch"}).indices
    assert idx == [1]

    idx = annot.reset_filter().filter({"tag": "pretty"}).indices
    assert idx == [0]

    # --- filter using keyword args ---
    idx = annot.reset_filter().filter(select=("SRKW", "*")).indices
    assert idx == [0]

    # note: finds descendant nodes too
    idx = annot.reset_filter().filter(select=("KW", "PC")).indices
    assert idx == [0, 1]

    idx = annot.reset_filter().filter(exclude=("SRKW", "*")).indices
    assert idx == []

    idx = annot.reset_filter().filter(exclude=("SRKW", "S01")).indices
    assert idx == []

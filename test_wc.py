
import pytest
import map_reduce
import master


def test_fail_word_count_map_reduce():
    mr_lib = map_reduce.MapReduceLibrary()
    response = mr_lib.init_cluster(3, 3)
    assert response.status_code == 200
    response = mr_lib.run_mapred(
        'projgut.txt',
        'word3_count1',
        'word3_count1',
        'word_count/results.txt'
    )
    assert response == ("Unknown Map Reduce Type", 400)


def test_success_word_count_map_reduce():
    mr_lib = map_reduce.MapReduceLibrary()
    response = mr_lib.init_cluster(3, 3)
    assert response.status_code == 200
    response = mr_lib.run_mapred(
        'projgut.txt',
        'word_count',
        'word_count',
        'word_count/results.txt'
    )
    assert response.status_code == 200

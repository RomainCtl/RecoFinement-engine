from pyspark import SparkConf, SparkContext
from .singleton import Singleton


class Spark(SparkContext, Singleton):
    def __init__(self, *args, **kwargs):
        conf = SparkConf().setAppName("RecoFinement_engine")
        super().__init__(*args, conf=conf, **kwargs)


def broadcast_matrix(mat):
    bcast = sc.broadcast((mat.data, mat.indices, mat.indptr))
    (data, indices, indptr) = bcast.value
    bcast_mat = csr_matrix((data, indices, indptr), shape=mat.shape)
    return bcast_mat


def parallelize_matrix(scipy_mat, rows_per_chunk=100):
    (rows, cols) = scipy_mat.shape
    i = 0
    submatrices = []
    while i < rows:
        current_chunk_size = min(rows_per_chunk, rows-i)
        submat = scipy_mat[i:i+current_chunk_size]
        submatrices.append(
            (i, (submat.data, submat.indices, submat.indptr), (current_chunk_size, cols)))
        i += current_chunk_size

    return sc.parallelize(submatrices)


def find_matches_in_submatrix(sources, targets, inputs_start_index, indices, threshold=.5, max_sim=10):
    cosimilarities = cosine_similarity(sources, targets)
    for i, cosimilarity in enumerate(cosimilarities):
        cosimilarity = cosimilarity.flatten()

        # Find real id
        source_index = indices[indices == inputs_start_index + i].index[0]
        if info[2] == int:
            source_index = int(source_index)

        # Sort by best match using argsort(), and take 10 first
        targets = cosimilarity.argsort()[-(max_sim+1):]

        for target_index in targets:
            similarity = cosimilarity[target_index]
            # Find real id
            target_index = indices[indices == target_index].index[0]
            if similarity >= threshold and target_index != source_index:
                if info[2] == int:
                    target_index = int(target_index)
                yield {"%s0" % info[1]: source_index, "%s1" % info[1]: target_index, "similarity": float(similarity)}


sc = Spark()

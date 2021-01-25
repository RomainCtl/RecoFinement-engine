from pyspark import SparkConf, SparkContext
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity

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


def find_matches_in_submatrix(sources, targets, inputs_start_index, indices, real_indice_name, content_type, threshold=.5, max_sim=10):
    cosimilarities = cosine_similarity(sources, targets)
    for i, cosimilarity in enumerate(cosimilarities):
        cosimilarity = cosimilarity.flatten()

        # Find real id
        source_indice = indices[indices == inputs_start_index + i].index[0]
        source_index = int(source_indice[0])
        source_type = source_indice[1]

        if str(source_type) != str(content_type[0]):
            continue

        # Sort by best match using argsort(), and take 10 first
        targets = cosimilarity.argsort()[-(max_sim+1):]

        for target_i in targets:
            similarity = cosimilarity[target_i]
            # Find real id
            target_indice = indices[indices == target_i].index[0]
            target_index = int(target_indice[0])
            target_type = target_indice[1]

            if str(target_type) != str(content_type[1]):
                continue

            if similarity >= threshold and target_index != source_index:
                yield {
                    "%s0" % real_indice_name: source_index,
                    "%s1" % real_indice_name: target_index,
                    "similarity": float(similarity),
                    "content_type0": str(content_type[0]).upper(),
                    "content_type1": str(content_type[1]).upper(),
                }


sc = Spark()

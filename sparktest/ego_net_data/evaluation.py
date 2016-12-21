#encoding=utf-8

def build_edge_list(filepath):
    with open(filepath) as f:
        lines = f.readlines()
    edge_list = dict()
    for line in lines:
        source, target = line.split(' ')
        if source > target:
            source, target = target, source
        edge_list[source + '_' + target] = 1
    return edge_list


def evaluate(truth, result):
    count = 0.0
    for key in result:
        if key in truth:
            count += 1
    return count


if __name__ == '__main__':
    delete_file = './facebook_combined_delete.txt'
    # recommand_file = './facebook_combined_delete.txt'
    recommand_file = './facebook_recommand.txt'
    delete_list = build_edge_list(delete_file)
    recommand_list = build_edge_list(recommand_file)
    overlap = evaluate(delete_list, recommand_list)
    print 'Precision: %s' % (overlap / len(recommand_list))
    print 'Recall: %s' % (overlap / len(delete_list))




from fuefpyco.it import consume, flatmap, fold_transform, unzip, windows


def test_flatmap() -> None:
    c = [1, 3, 2, 4]
    r = flatmap(lambda v: [v] if v > 2 else [], c)
    assert list(r) == [3, 4]


def test_fold_transform() -> None:
    pipeline = [
        lambda a: a + 1,
        lambda a: a * 2,
    ]
    assert fold_transform(2, pipeline) == 6


def test_unzip() -> None:
    l1 = [1, 2, 3]
    l2 = ["a", "b", "c"]
    zl = zip(l1, l2)
    rl1, rl2 = unzip(zl)
    assert (list(rl1), list(rl2)) == (l1, l2)


def test_consume() -> None:
    ls = [1, 2, 3, 4, 5]
    li = iter(ls)
    nil, li = consume(li, 0)
    assert nil == []
    head, li = consume(li, 1)
    assert head[0] == 1
    body, li = consume(li, 2)
    assert body == [2, 3]
    assert list(li) == [4, 5]


def test_windows() -> None:
    ls = [1, 2, 3, 4, 5]
    badly_consumed = list(windows(ls, 2))
    badly_consumed_eval = [list(e) for e in badly_consumed]
    assert badly_consumed_eval == [[1], [2], [3], [4], [5]]
    well_consumed = []
    for window in windows(ls, 2):
        well_consumed.append(list(window))
    assert well_consumed[0] == [1, 2]
    assert well_consumed[1] == [3, 4]
    assert well_consumed[2] == [5]

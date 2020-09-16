# encoding: utf-8
'''
@author: **
@license: Apache Licence 
@software: PyCharm
@file: cache_for_es.py
@time: 2020/6/18 6:20 PM
'''
from __future__ import division
import redis
import time
import random
import threading
from multiprocessing import Queue, Process, Pool
import numpy as np
from collections import OrderedDict

que = Queue()
N = 1
GAP = 5
tst = redis.Redis(host='127.0.0.1', port=10086, db=1)
tst3 = redis.Redis(host='127.0.0.1', port=32768, db=2)

__doc__ = '重复数据并发写入数据库！' \
          '使用redis先接受多线程输入的数据，然后比较这些数据是否有相似的，' \
          '如果有，则延后；如没有，则直接进入下一环节。也就是认为拉开相似者的距离' \
          '1. 先比较时间差异是否足够大，是就跳过' \
          '2. 再比较汉明距离是否足够大，是就跳过' \
          '3. 都不满足，比较时间戳，先进先出' \
          '4. 后出者等待对方执行完或者一段时间' \
          '----------------------------' \
          '这个就是测试demo' \
          '----------------------------' \
          '补充：worker是普通iid对应hash' \
          'worker2是hash做key，用了scan和mget' \
          'worker3是iid做key，用hset和hgetall来取值'
__all__ = ['worker', 'worker2', 'worker3', 'cal_hamming']


def cal_hamming(a, b):
    count = 0
    for x, y in zip(a, b):
        for i in '{:064b}'.format(x ^ y):
            if i == '1':
                count += 1
    return count


def get_time():
    return time.time() + random.uniform(0, 0.0000001)


def worker2(iid, uhash, ques):
    # hash值做key
    # 0.25 to pull later
    time.sleep(random.uniform(0, 0.03))
    start = time.time()
    status = False  # has_paired
    n_status = 0
    n_aimss = 0
    n_looks = 0
    checked = {iid}
    hm_dis = {}
    MAX_HAMMING = 7
    aim_hash = ''
    during = OrderedDict()

    def wait4res_2(key):
        cc = tst.get(key)
        if cc is None or eval(cc)[0] is None:
            time.sleep(0.05)
            return None
        # elif eval(cc)[0] is None:
        #     _,stamp=eval(cc)[1]
        #     time.time()-stamp
        else:
            _fp = eval(cc)[0]
            # 与人方便
            _ = tst.expire(key, 2)
            # 与己方便
            if key != str_hash:
                tst.set(str_hash, (_fp, time.time(), iid))
                _ = tst.expire(str_hash, 2)
            return _fp

    def es_mimic(_hash, fp=None):
        if fp is None:
            # 模仿es耗时
            time.sleep(0.05)
            # post action
            _fp = random.randint(0, 1e8)
            # _ = tst.delete(iid)
            tst.set(_hash, (_fp, time.time(), iid))
            _ = tst.expire(_hash, 2)
        else:
            _fp = fp
        all_ = time.time() - start
        during['all'] = all_
        No = n_looks * 10000 + n_status + n_aimss / 100.0
        ques.put(('%s--%s' % (iid, _fp), values, No, during))

    ##########
    pre_ = time.time()
    byte_hash, str_hash = get_divid_hash(uhash)
    ustamp = time.time()
    values = (uhash, ustamp)
    if not tst.exists(str_hash):
        # 先占坑，然后找相似，或者自己来
        tst.set(str_hash, (None, ustamp, iid))
        _ = tst.expire(str_hash, 2)
    else:
        res = tst.get(str_hash)
        try:
            fp, _, _ = eval(res)
        except Exception as e:
            during['Exception'] = (res, str_hash)
            fp = None

        if fp is not None:
            during['fn'] = 1
            es_mimic(str_hash, fp)
            return
        else:
            status = True
            aim_hash = str_hash

    during['pre'] = time.time() - pre_
    time.sleep(0.015)  # generate iid,get pika,etc.
    # 后来者可能可以直接从pika中读到数据，并直接返回，并重新set redis等修改时间戳的操作
    if not status:
        # first find record have been changed 被篡改
        res = tst.get(str_hash)
        _value = eval(res)
        # if _value[0] is not None:
        #     during['fn'] = 5
        #     es_mimic(str_hash, _value[0])
        #     return
        if abs(ustamp - _value[1]) > 0.000001:
            status = True
            aim_hash = str_hash
            during['xxx'] = ustamp - _value[1]
        else:
            # find out those have similar hash values
            mid_ = time.time()
            all_items = tst.scan(count=1000)[1]  # at least has itself
            all_values = []
            for iii, ii in enumerate(tst.mget(all_items)):
                tmp = eval(ii) if ii else ii
                all_values.extend([tmp])
            # n_status = len(all_values)
            during['mid'] = time.time() - mid_
            end_ = time.time()
            # 70ms for 3000
            for _key, _value in zip(all_items, all_values):
                if not _value or len(_value) != 3: continue  # bad data
                if str_hash == _key:
                    # if _value[0] is not None:
                    #     during['fn'] = 4
                    #     es_mimic(str_hash, _value[0])
                    #     return
                    if abs(ustamp - _value[1]) > 0.000001:
                        # twice check
                        status = True
                        aim_hash = str_hash
                        during['xxy'] = ustamp - _value[1]
                        break
                    else:
                        continue
                if ustamp < _value[1]: continue
                # should checked if equals
                n_status += 1
                ct = 0
                for x, y in zip(_key, str_hash):
                    if x != y: ct += 1
                    if ct > 10: break
                if ct < 10:
                    # n_looks += 1
                    aim_hash = _key
                    status = True
                    during['target'] = _value[2]
                    break
            during['end'] = time.time() - end_

    if status:
        # must have aim_hash
        # wait! has exists#suppose no error occur
        trycount = 0
        fp = None
        # 死等
        while trycount < 10:
            n_aimss += 1
            trycount += 1
            fp = wait4res_2(aim_hash)
            if fp: break
        during['fn'] = 2
        es_mimic(aim_hash, fp)
        return
    else:
        # go into es
        # time.sleep(0.02)
        during['fn'] = 3
        es_mimic(str_hash)
        return


def wait4res(key):
    start = time.time()
    while time.time() - start < 0.055:
        cc = tst.get(key)
        if cc is None:
            time.sleep(0.03)
        else:
            _ = tst.expire(key, 1)
            return cc
    return


# 直接就能根据汉明距离找到的pika中已存储的数据的条目，那就直接返回结果
# 找不到的，那就看：1.是否有排在自己前面的，如果有汉明距离又很近的，等他的结果；
#               2.如果有但是汉明距离不足够近的，排一下队进入es
#               3.如果没有，那就直接进入es流程
def worker(iid, uhash, ques):
    # 常规的字典方式
    # 如果使用这个的话，还需要将相同的后面几个不经es直接返回的模块
    # 0.25 to pull later
    if random.randint(1, 4) < 2:
        time.sleep(0.03)
    start = time.time()
    ustamp = time.time()
    key_name = int(ustamp)
    values = (uhash, ustamp)
    key_hash = '_'.join([str(i) for i in uhash])
    #########
    tst.set(iid, values)
    _ = tst.expire(iid, 1)
    time.sleep(0.005)
    status = False
    n_status = 0
    n_aimss = 0
    n_looks = 0
    checked = {iid}
    hm_dis = {}
    aims_hash = ''
    fp = tst.get(key_hash)
    all_items = tst.scan(count=1000)[1]
    if all_items:
        local_rds = dict(zip(all_items, tst.mget(all_items)))
    else:
        time.sleep(0.05)
        fp = random.randint(0, 1e8)
    while not fp:
        for k in all_items:
            # if k == iid: continue
            if k in checked: continue
            if '_' in k:
                checked.add(k)
                continue
                # kk = k.split('_')
                # khash = [int(i) for i in kk[:2]]
                # k_ham = cal_hamming(uhash, khash)
                # if k_ham < 3:
                #     # aims_hash = '_'.join([str(i) for i in khash])
                #     fp = tst.get(aims_hash)
                #     break
                # else:
                #     checked.add(k)
                #     continue
            n_looks += 1
            cc = local_rds.get(k, None)
            # if not cc:
            #     cc = tst.get(k)
            #     local_rds[k] = cc
            if not cc:
                checked.add(k)
                continue
            khash, stamp = eval(cc)
            if ustamp < stamp:
                # earlier whatever
                checked.add(k)
                continue
            dis_ham = hm_dis.get(k, None)
            if not dis_ham:
                dis_ham = cal_hamming(uhash, khash)
                hm_dis[k] = dis_ham
            if dis_ham > 10:
                # this item has no influence
                checked.add(k)
                continue
            # this matters,there's someone in head of u
            # and remember the frontier
            status = True
            if dis_ham < 3:
                aims_hash = '_'.join([str(i) for i in khash])
            break
        if fp:
            break
        if aims_hash:
            n_aimss += 1
            # _ = tst.delete(iid)
            # wait for results, but may be None
            # 暂时没管第一个没有有效返回值；
            fp = wait4res(aims_hash)
            # 最外层用while循环是防止这儿取不到值，又不能直接进入es
            # （可能前者没执行es，又有后者，那就重新进入一下吧）
            # 不过到了这儿已经等了有一会了（55ms起），不知的目前这项是排在了第二还是第三位置，所以还要等等
            if fp:
                break
        elif status:
            time.sleep(0.01)
            n_status += 1
            # similiar but not same, just go!
            if n_status > 1:
                break
            status = False
        else:
            # first of all, check that whether it exists
            fp = tst.get(key_hash)
            if fp: break
            # then delete    iid in pika buff
            # 1. will get es result; 2. get error;
            # so do not stop others' procedure
            # _ = tst.delete(iid)
            # suppose it exhaust by es
            time.sleep(0.05)
            _ = tst.expire(iid, 3)
            # post action
            fp = random.randint(0, 1e8)
            tst.set(key_hash, fp)
            _ = tst.expire(key_hash, 3)
            break
    during = time.time() - start
    No = n_looks * 1000 + n_status + n_aimss / 100.0
    ques.put(('%s--%s' % (iid, fp), values, No, during))


def get_divid_hash(uhash):
    res, res_str = [], ''
    for h in uhash:
        # _str = bin(h).replace('0b', '')
        _str = '{:064b}'.format(h)
        res_str += _str
        res.extend([int(i) for i in _str])
    return res, res_str


def worker3(iid, uhash, ques):
    # 常规的字典方式
    # 如果使用这个的话，还需要将相同的后面几个不经es直接返回的模块
    # 0.25 to pull later
    time.sleep(random.uniform(0, 0.03))

    start = time.time()
    ustamp = time.time()
    values = (uhash, ustamp)
    #########
    key_name = int(ustamp)
    LIFE = 1
    if 1 != LIFE:
        key_name = key_name // LIFE * LIFE
    key_name_fp = '%d_fp' % key_name
    pre_key_name = key_name - LIFE
    pre_key_name_fp = '%d_fp' % pre_key_name
    byte_hash, str_hash = get_divid_hash(uhash)
    tst.hset(key_name, iid, (str_hash, ustamp))
    _ = tst.expire(key_name, LIFE)

    # time.sleep(0.005)
    status = False
    n_status = 0
    n_aimss = 0
    n_looks = 0
    checked = {iid}
    hm_dis = {}
    MAX_HAMMING = 10
    during = {}

    def is_current_fp(stamp):
        xx_name = int(stamp)
        if 1 != LIFE:
            xx_name = xx_name // LIFE * LIFE
        return xx_name == key_name

    def wait4res_3(_hash_name, key):
        cc = tst.hget(_hash_name, key)
        if cc is None:
            time.sleep(0.03)
            return None
        # elif eval(cc)[0] is None:
        #     _,stamp=eval(cc)[1]
        #     time.time()-stamp
        else:
            _fp = cc
            # 与人方便
            _ = tst.expire(_hash_name, 2 * LIFE)
            # 与己方便
            if key != str_hash or _hash_name != key_name_fp:
                tst.hset(key_name_fp, str_hash, _fp)
                _ = tst.expire(key_name_fp, 2 * LIFE)
            return _fp

    def es_mimic_2(_hash_name, fp=None):
        if fp is None:
            # 模仿es耗时
            time.sleep(0.05)
            # post action
            _fp = random.randint(0, 1e8)
            # _ = tst.delete(iid)
            tst.hset(_hash_name, str_hash, _fp)
            _ = tst.expire(_hash_name, 2 * LIFE)
        else:
            _fp = fp
            if _hash_name != key_name_fp:
                tst.hset(key_name_fp, str_hash, _fp)
                _ = tst.expire(key_name_fp, 2 * LIFE)
        all_ = time.time() - start
        during['all'] = all_
        No = n_looks * 1000 + n_status + n_aimss / 100.0
        ques.put(('%s--%s' % (iid, _fp), values, No, during))

    def distance(xx, yy):
        ct = 0
        for x, y in zip(xx, yy):
            if x != y: ct += 1
            if ct > MAX_HAMMING: break
        return ct

    #########

    _name = key_name_fp
    # try:
    fp = tst.hget(key_name_fp, str_hash)
    # except Exception as e:
    #     print(e, key_name_fp, str_hash)
    #     raise e
    if not fp:
        _name = pre_key_name_fp
        fp = tst.hget(pre_key_name_fp, str_hash)
    if fp:
        during['fun'] = 1
        es_mimic_2(_name, fp)
        return

    _pre = time.time()
    time.sleep(0.01)
    all_items = {}
    pre_tmp = tst.hgetall(pre_key_name)
    cur_tmp = tst.hgetall(key_name)
    if pre_tmp: all_items.update(pre_tmp)
    if cur_tmp: all_items.update(cur_tmp)
    #
    all_fps = {}
    pre_fp = tst.hgetall(pre_key_name_fp)
    cur_fp = tst.hgetall(key_name_fp)
    if pre_fp: all_fps.update(pre_fp)
    if cur_fp: all_fps.update(cur_fp)
    during['pre'] = time.time() - _pre
    #
    # all_values = tst.mget(all_items)
    n_status = len(all_items)
    if not all_items:
        during['fun'] = 2
        es_mimic_2(key_name_fp)
        return
    else:
        # rids_items, rids_stamps, hash_items = [], [], []
        # rids_mapping, hash_mapping = [], []

        _mid = time.time()
        # 先看有没有现成的可用
        for key in all_fps:
            ct = distance(key, str_hash)
            value = all_fps[key]
            # n_looks+=1
            if ct < MAX_HAMMING:
                # during['target'] = _value[2]
                during['fun'] = 3
                _name_fp = pre_key_name_fp if key in pre_tmp else key_name_fp
                # 尽量确保现在这个包里面有
                es_mimic_2(_name_fp, all_fps[key])
                during['mid'] = time.time() - _mid
                return
        during['mid'] = time.time() - _mid
        _end = time.time()
        # 记录那些距离不够近的样例，如果有更小的距离，那就自己处理了，用不到这个项
        # 不然，这个记录就是时间比此项早，距离又够近的情况。反正都是早，按距离排序，最后不行了就让此项顶上
        far_name = []  # far_dis,far_key,far_n=100,'',0
        # 没有现成的就来找队友
        for idx, key in enumerate(all_items):
            if iid == key: continue
            value = eval(all_items[key])
            # if not value: continue
            n_status += 1
            _hash, _stamp = value
            # FIFO
            if ustamp < _stamp:
                continue
            hamming = distance(str_hash, _hash)
            # 应该要考虑那些汉明距离较大的情况[3,10]，让他们排队进es。
            # 但是就假设局部（2s）的情况下，这种不存在。
            # 因为排队会大大延后这种项，不如不等的整体收益高
            _name_fp = key_name_fp if is_current_fp(_stamp) else pre_key_name_fp
            if hamming > MAX_HAMMING:
                # go to es
                continue
            elif hamming > MAX_HAMMING / 2:
                # 相近但是又有距离的，先看看选择排队，也就是全看
                # 如果排除经过了hamming<MAX_HAMMING / 2的情况，那那种情况已经自己搞定自己了
                # time.sleep(0.02)
                # far_name = pre_key_name_fp if key in pre_tmp else key_name_fp
                # during['target'] = key  # '%.4f__%s' % (_stamp, far_name)
                # break
                status = True
                n_looks += 1
                far_name.append([hamming, _name_fp, _hash, key])
            else:
                # 已经很近了，就别乱看了，就是这个了
                status = True
                # if key in pre_tmp:
                #     _name_fp = pre_key_name_fp
                # else:
                #     _name_fp = key_name_fp
                # _name_fp = key_name_fp if is_current_fp(_stamp) else pre_key_name_fp
                # wait for results of first one,or rely next one
                during['target'] = key  # '%.4f__%s' % (_stamp, _name_fp)
                trycount = 0
                while trycount < 10:
                    n_aimss += 1
                    fp = wait4res_3(_name_fp, _hash)
                    if fp: break
                    trycount += 1
                if fp:
                    during['fun'] = 4
                    es_mimic_2(key_name_fp, fp)
                    during['end'] = time.time() - _end
                    return
        if status:
            # 分两种情况，带相近的项,far_name可能是空的
            far_name.sort(key=lambda x: x[0])
            for _dis, _name_fp, _hash, _key in far_name:
                during['target'] = _key  # '%.4f__%s' % (_stamp, _name_fp)
                trycount = 0
                while trycount < 10:
                    fp = wait4res_3(_name_fp, _hash)
                    if fp: break
                    trycount += 1
                if fp:
                    during['fun'] = 7
                    es_mimic_2(key_name_fp, fp)
                    during['end'] = time.time() - _end
                    return
            # 被最近距离那流放过来，或者上面流放过来，都直接执行下面的吧
            # 不过这儿的排队，顺序的ok的，但是时间、时机都不好。
            # 等第一个等了很久，冷静一下，进入esgo into es
            time.sleep(0.01)
            during['fun'] = 5
        else:
            during['fun'] = 6
        es_mimic_2(key_name_fp)
        during['end'] = time.time() - _end


###########################################################################################
#                                                                                         #
###########################################################################################
def dbsz():
    print(tst.dbsize())
    # print(list(tst.scan_iter()))


def check():
    res = {}
    all_info = []
    while not que.empty():
        iid, values, n_status, tm = que.get()
        hashes, stamp = values
        res[iid] = {'n': n_status, 'eclipse': tm, 'hash': hashes, 'stamp': stamp}
    read = set()
    normal, alln = 0, 0.0
    for k in res:
        FP = set()
        if k in read: continue
        infoa = []
        read.add(k)
        uhash = res[k]['hash'][:2]
        tm_ = res[k]['eclipse']
        if isinstance(tm_, dict):
            tm = tm_['all']
            others = tm_
        else:
            tm = tm_
            others = None
        stamp = res[k]['stamp']
        infoa.append('key: %s, N: %s, tm: %.3f, eclipse: %.3f, stamp: %.4f, hash: %s, others:%s' % \
                     (k, format(res[k]['n'], ','), 0, tm, stamp, uhash, others))
        FP.add(k.split('-')[-1])
        for j in res:
            if j in read: continue
            khash = res[j]['hash'][:2]
            dis_ham = cal_hamming(uhash, khash)
            if dis_ham < 10:
                read.add(j)
                tmj_ = res[j]['eclipse']
                if isinstance(tmj_, dict):
                    tmj = tmj_['all']
                    others = tmj_
                else:
                    tmj = tmj_
                    others = None
                stampj = res[j]['stamp']
                infoa.append('key: %s, N: %s, tm: %.3f, eclipse: %.3f, stamp: %.4f, hash: %s, DIS: %d, others:%s' \
                             % (j, format(res[j]['n'], ','), tmj - tm, tmj, stampj, khash, dis_ham, others))
                FP.add(j.split('-')[-1])
        mark = -1
        if len(infoa) > 0:
            mark = 0
        if len(infoa) > 1:
            mark = 1
        if len(infoa) > 1 and len(FP) != 1:
            mark = 2
        if mark > -1: all_info.append((mark, infoa, FP))
        if len(infoa) > 1:
            alln += 1
            if len(FP) == 1:
                normal += 1

    condtion = {'All': 0, 'Multi': 1, 'Abnormal': 2}
    all_info.sort(key=lambda x: x[0])
    for infoas in all_info:
        if condtion[PC] != infoas[0]: continue
        mark, infoa, FP = infoas
        for info in infoa:
            print(info)
        print('-' * 20, len(infoa))
        if mark == 2: print('%' * 20, FP)
    print('There are %d multiple lines is ok, about %.3f.' % (normal, 1.0 * normal / alln))


def test1(data, thrd_fun):
    ps = []
    for a, b in data:
        t = threading.Thread(target=thrd_fun, args=(a, b, que))
        t.start()
        ps.append(t)
    for t in ps:
        t.join()


def test2(nt, thrd_fun):
    ps = []
    for i in range(nt):
        p = Process(target=thrd_fun, args=(que,))
        ps.append(p)
    for p in ps:
        p.start()
    for p in ps:
        p.join()


def test3(nt, thrd_fun):
    ps = Pool(5)
    for a, b in gen_data(nt):
        ps.apply_async(thrd_fun, args=(a, b, que))
    try:
        print("Waiting ... ...")
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        ps.terminate()
        ps.join()
    else:
        ps.close()
        ps.join()


####################################################################
def get_hash():
    s = np.random.uniform(0, 1, 64) > 0.5
    ss = ''.join(map(str, s.astype(np.byte).tolist()))
    b = int(ss, 2)
    return b


def gen_data(n):
    stamp = time.time() - 1
    tmp_hash = ''
    incre = np.random.uniform(0, 0.1, n)
    incre.sort()
    res = []
    for i in range(n):
        hashes = [get_hash(), get_hash()]
        if i > 0 and random.randint(1, 10) > 6:
            hashes = tmp_hash
        iid = '%s' % random.randint(1, 1e8)
        ustamp = stamp + incre[i]
        tmp_hash = hashes
        # yield (iid, hashes)
        res.append([iid, hashes])
    return res


def get_hash2(ssa, ssb):
    if random.randint(1, 2) > 1:
        N = np.random.randint(2, 5)
    else:
        N = np.random.randint(20, 30)
    a_idx = np.unique(np.random.random_integers(0, 63, 2))
    b_idx = np.unique(np.random.random_integers(0, 63, (N - len(a_idx))))
    a_val = np.random.random_integers(0, 1, len(a_idx))
    b_val = np.random.random_integers(0, 1, len(b_idx))
    ssa[a_idx] = a_val
    ssb[b_idx] = b_val
    a_int = int(''.join(map(str, ssa.tolist())), 2)
    b_int = int(''.join(map(str, ssb.tolist())), 2)
    return a_int, b_int


def gen_data2(n):
    stamp = time.time() - 1
    tmp_hash = ''
    incre = np.random.uniform(0, 0.1, n)
    incre.sort()
    res = []
    sa = (np.random.uniform(0, 1, 64) > 0.5).astype(np.byte)
    sb = (np.random.uniform(0, 1, 64) > 0.5).astype(np.byte)
    for i in range(n):
        hashes = get_hash2(sa, sb)
        # print(hashes, sa.sum(), sb.sum())
        if i > 0 and random.randint(1, 10) > 6:
            hashes = tmp_hash
        iid = '%s' % random.randint(1, 1e8)
        ustamp = stamp + incre[i]
        tmp_hash = hashes
        # yield (iid, hashes)
        res.append([iid, hashes])
    return res


####################################################################
def main(nt, thrd_fun):
    test = [test1]
    data = gen_data2(nt)
    for _ in range(5): print('# ' * 20)
    for fun in test:
        start = time.time()
        fun(data, thrd_fun)
        ecli = time.time() - start
        print('que.empty:', que.empty())
        dbsz()
        check()
        print('que.empty:', que.empty())
        # time.sleep(0.05)
        print('#' * 20, ecli)


def main2(nt):
    data = gen_data(nt)
    start = time.time()
    for a, b in data:
        worker3(a, b, que)
    ecli = time.time() - start
    print('que.empty:', que.empty())
    dbsz()
    check()
    print('que.empty:', que.empty(), ecli)


if __name__ == '__main__':
    # from cache_query import work4
    Print_checks = ['All', 'Multi', 'Abnormal']
    PC = Print_checks[1]
    main(1000, worker3)
    # main2(300)

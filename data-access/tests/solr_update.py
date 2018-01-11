

import solr

solrcon = solr.Solr('http://%s/solr/%s' % ('localhost:8983', 'nexustiles'))

ds = 'MXLDEPTH_ECCO_version4_release1'

# print solrcon.select(q='dataset_s:%s' % ds, sort='id', cursorMark='*').results


params = {'q': 'dataset_s:%s' % ds, 'sort': 'id', 'cursorMark': '*', 'rows': 5000}
done = False
while not done:
    response = solrcon.select(**params)
    print len(response.results)
    if params['cursorMark'] == response.nextCursorMark:
        done = True

    params['cursorMark'] = response.nextCursorMark

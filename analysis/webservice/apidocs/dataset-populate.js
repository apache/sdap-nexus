// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


function DatasetPopulatePlugin(system) {
  const DS_PATH = '/domslist'
  const ACTION_UPDATE_DATASETS = 'UPDATE_DATASETS'
  const ACTION_UPDATE_ERROR = 'UPDATE_ERROR'

  async function loadDatasets() {
    try {
      let response = await fetch(DS_PATH)
      if (!response.ok) {
        system.dsPopulateActions.updateError(true)
        console.error(`[DatasetPopulate] Unable to load dataset list from: ${DS_PATH}; Status: ${response.status}`)
      }

      let results = await response.json()
      let satellite = results.data.satellite
      let insitu = results.data.insitu

      let total = 0
      let datasets = {
        satellite: [],
        insitu: []
      }

      for (const ds of satellite) {
        datasets['satellite'].push(ds.shortName)
        total++
      }

      for (const ds of insitu) {
        datasets['insitu'].push(ds.name)
        total++
      }

      datasets['satellite'].sort()
      datasets['insitu'].sort()

      system.dsPopulateActions.updateDatasets(datasets)
      console.debug(`[DatasetPopulate] Lists populated; count: ${total}`)
    } catch (err) {
      system.dsPopulateActions.updateError(true)
      console.error(`[DatasetPopulate] Error retreiving dataset list: ${err.message}`);
    }
  }

  return {
    statePlugins: {
      dsPopulate: {
        actions: {
          updateDatasets: (list) => {
            return {
              type: ACTION_UPDATE_DATASETS,
              payload: list
            }
          },
          updateError: (error) => {
            return {
              type: ACTION_UPDATE_ERROR,
              payload: error
            }
          }
        },
        reducers: {
          [ACTION_UPDATE_DATASETS]: (state, action) => state.set('datasets', action.payload),
          [ACTION_UPDATE_ERROR]: (state, action) => state.set('error', action.payload)
        },
        selectors: {
          datasets: (state) => state.get('datasets'),
          hasError: (state) => state.get('error')
        }
      }
    },
    wrapComponents: {
      JsonSchemaForm: (Original, system) => (props) => {
        let dsPopulate = props.schema.get('x-dspopulate') ? props.schema.get('x-dspopulate') : false

        if (!dsPopulate || system.dsPopulateSelectors.hasError())
          return system.React.createElement(Original, props)

        let datasetMap = system.dsPopulateSelectors.datasets()
        if (!datasetMap)
          return system.React.createElement(Original, props) 

        let datasets = []
        for (const datasetName of dsPopulate.toArray()) {
          datasets.push(...datasetMap[datasetName])
        }

        datasets.sort()

        props.schema = props.schema.set('enum', datasets)

        if (!(datasets.includes(props.value))) {
          props.value = ""
          props.onChange("")
        }

        return system.React.createElement(Original, props)
      }
    },
    afterLoad: (system) => {
      system.dsPopulateActions.updateError(false)
      loadDatasets()
    }
  }
}

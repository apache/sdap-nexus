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

      let responseJson = await response.json()
      let satellite = responseJson.data.satellite
      let datasets = []

      for (const ds of satellite)
        datasets.push(ds.shortName)

      datasets.sort()

      system.dsPopulateActions.updateDatasets(datasets)
      console.debug(`[DatasetPopulate] List populated; count: ${datasets.length}`)
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
        const JsonSchema_string = system.getComponent('JsonSchema_string')
        let dsPopulate = props.schema.get('x-dspopulate') ? props.schema.get('x-dspopulate') : false

        if (!dsPopulate || system.dsPopulateSelectors.hasError())
          return system.React.createElement(Original, props)

        let datasets = system.dsPopulateSelectors.datasets()
        props.schema = props.schema.set('enum', datasets)
        return system.React.createElement(JsonSchema_string, props)
      }
    },
    afterLoad: (system) => {
      system.dsPopulateActions.updateError(false)
      loadDatasets()
    }
  }
}
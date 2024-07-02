#!/bin/bash

GREP_OPTIONS=''

cookiejar=$(mktemp cookies.XXXXXXXXXX)
netrc=$(mktemp netrc.XXXXXXXXXX)
chmod 0600 "$cookiejar" "$netrc"
function finish {
  rm -rf "$cookiejar" "$netrc"
}

trap finish EXIT
WGETRC="$wgetrc"

prompt_credentials() {
    echo "Enter your Earthdata Login or other provider supplied credentials"
    read -p "Username (rileykk): " username
    username=${username:-rileykk}
    read -s -p "Password: " password
    echo "machine urs.earthdata.nasa.gov login $username password $password" >> $netrc
    echo
}

exit_with_error() {
    echo
    echo "Unable to Retrieve Data"
    echo
    echo $1
    echo
    exit 1
}

if [ -z "${DATA_DIRECTORY}" ]; then
  exit_with_error "DATA_DIRECTORY variable unset or empty. This is needed so we know where to store the data"
fi

prompt_credentials
  detect_app_approval() {
    approved=`curl -s -b "$cookiejar" -c "$cookiejar" -L --max-redirs 5 --netrc-file "$netrc" https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-10-02.nc -w '\n%{http_code}' | tail  -1`
    if [ "$approved" -ne "200" ] && [ "$approved" -ne "301" ] && [ "$approved" -ne "302" ]; then
        # User didn't approve the app. Direct users to approve the app in URS
        exit_with_error "Provided credentials are unauthorized to download the data"
    fi
}

setup_auth_curl() {
    # Firstly, check if it require URS authentication
    status=$(curl -s -z "$(date)" -w '\n%{http_code}' https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-10-02.nc | tail -1)
    if [[ "$status" -ne "200" && "$status" -ne "304" ]]; then
        # URS authentication is required. Now further check if the application/remote service is approved.
        detect_app_approval
    fi
}

setup_auth_wget() {
    # The safest way to auth via curl is netrc. Note: there's no checking or feedback
    # if login is unsuccessful
    touch ~/.netrc
    chmod 0600 ~/.netrc
    credentials=$(grep 'machine urs.earthdata.nasa.gov' ~/.netrc)
    if [ -z "$credentials" ]; then
        cat "$netrc" >> ~/.netrc
    fi
}

fetch_urls() {
  download_dir=${DATA_DIRECTORY}/$collection
  echo mkdir -p $download_dir
  mkdir -p $download_dir

  if command -v curl >/dev/null 2>&1; then
      setup_auth_curl
      while read -r line; do
        # Get everything after the last '/'
        filename="${line##*/}"

        # Strip everything after '?'
        stripped_query_params="${filename%%\?*}"

        curl -f -b "$cookiejar" -c "$cookiejar" -L --netrc-file "$netrc" -g -o $download_dir/$stripped_query_params -- $line && echo || exit_with_error "Command failed with error. Please retrieve the data manually."
      done;
  elif command -v wget >/dev/null 2>&1; then
      # We can't use wget to poke provider server to get info whether or not URS was integrated without download at least one of the files.
      echo
      echo "WARNING: Can't find curl, use wget instead."
      echo "WARNING: Script may not correctly identify Earthdata Login integrations."
      echo
      setup_auth_wget
      while read -r line; do
        # Get everything after the last '/'
        filename="${line##*/}"

        # Strip everything after '?'
        stripped_query_params="${filename%%\?*}"

        wget --load-cookies "$cookiejar" --save-cookies "$cookiejar" --output-document $download_dir/$stripped_query_params --keep-session-cookies -- $line && echo || exit_with_error "Command failed with error. Please retrieve the data manually."
      done;
  else
      exit_with_error "Error: Could not find a command-line downloader.  Please install curl or wget"
  fi
}

collection="MUR25-JPL-L4-GLOB-v04.2_test"

fetch_urls <<'EDSCEOF'
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20181001090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180930090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180929090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180928090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180927090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180926090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180925090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180924090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180923090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180922090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180921090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180920090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180919090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180918090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180917090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180916090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180915090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180914090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180913090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180912090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180911090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180910090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180909090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180908090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180907090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180906090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180905090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180904090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180903090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180902090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180901090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180831090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180830090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180829090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180828090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180827090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180826090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180825090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180824090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180823090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180822090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180821090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180820090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180819090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180818090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180817090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180816090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180815090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180814090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180813090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180812090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180811090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180810090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180809090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180808090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180807090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180806090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180805090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180804090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180803090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180802090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180801090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180731090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180730090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180729090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180728090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180727090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180726090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180725090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180724090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180723090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180722090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180721090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180720090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180719090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180718090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180717090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180716090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180715090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180714090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180713090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180712090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180711090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180710090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180709090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180708090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180707090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180706090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR25-JPL-L4-GLOB-v04.2/20180705090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc
EDSCEOF

collection="ASCATB_L2_Coastal_test"

fetch_urls <<'EDSCEOF'
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180705_053300_metopb_30070_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180705_172400_metopb_30077_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180801_025100_metopb_30452_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180801_144200_metopb_30459_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180801_162400_metopb_30460_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180802_023000_metopb_30466_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180802_041200_metopb_30467_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180802_160300_metopb_30474_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180924_073900_metopb_31222_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180924_091800_metopb_31223_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180924_210900_metopb_31230_eps_o_coa_2401_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180925_085700_metopb_31237_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180925_204800_metopb_31244_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180926_083600_metopb_31251_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180926_202700_metopb_31258_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180926_220900_metopb_31259_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180927_081500_metopb_31265_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180927_095700_metopb_31266_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180927_200600_metopb_31272_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180927_214800_metopb_31273_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180928_075400_metopb_31279_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180928_093600_metopb_31280_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180928_194500_metopb_31286_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180928_212700_metopb_31287_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180929_091500_metopb_31294_eps_o_coa_3201_ovw.l2.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/ASCATB-L2-Coastal/ascat_20180929_210600_metopb_31301_eps_o_coa_3201_ovw.l2.nc
EDSCEOF

collection="VIIRS_NPP-2018_Heatwave_test"

fetch_urls <<'EDSCEOF'
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/VIIRS_NPP-JPL-L2P-v2016.2/20180705204800-JPL-L2P_GHRSST-SSTskin-VIIRS_NPP-D-v02.0-fv01.0.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/VIIRS_NPP-JPL-L2P-v2016.2/20180705093001-JPL-L2P_GHRSST-SSTskin-VIIRS_NPP-N-v02.0-fv01.0.nc
EDSCEOF

collection="OISSS_L4_multimission_7day_v1_test"

fetch_urls <<'EDSCEOF'
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-10-02.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-09-28.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-09-24.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-09-20.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-09-16.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-09-12.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-09-08.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-09-04.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-08-31.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-08-27.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-08-23.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-08-19.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-08-15.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-08-11.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-08-07.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-08-03.nc
https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/OISSS_L4_multimission_7day_v1/OISSS_L4_multimission_global_7d_v1.0_2018-07-30.nc
EDSCEOF

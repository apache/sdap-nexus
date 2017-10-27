c
c     gaussInterp routine -- Gaussian weighted smoothing in lat, lon, and time
c
c Based on Ed Armstrong's routines. Designed to be called from python using f2py.
c
c
c Gaussian weighting = exp( vfactor * (((x - x0)/sx)^2 + ((y - y0)/sy)^2 + ((t - t0)/st)^2 ))
c
c where deltas are distances in lat, lon and time and sx, sy, st are one e-folding sigmas.
c
c Cutoffs for neighbors allowed in the interpolation are set by distance in lat/lon (see wlat/wlon);
c for time all epochs are included.


      subroutine gaussInterp_f(var, mask,
     &                         time, lat, lon,
     &                         outlat, outlon,
     &                         wlat, wlon,
     &                         slat, slon, stime,
     &                         vfactor,
     &                         missingValue,
     &                         verbose,
     &                         vinterp, vweight, status,
     &                         ntime, nlat, nlon, noutlat, noutlon)

      implicit none
      integer*4 ntime, nlat, nlon
c                 ! prepared 3D array of variable data over time, lon, & lat
      real*4    var(ntime, nlat, nlon)
c                 ! variable to be interpolated, over ntime epochs
      integer*1 mask(ntime, nlat, nlon)
c                 ! pixel quality mask
      integer*4 time(ntime)
c                 ! time epochs to gaussian-interpolate over
      real*4 lat(nlat)
                  ! latitude corrdinate vector
      real*4 lon(nlon)
                  ! longitude corrdinate vector
cf2py intent(in) var, mask, time, lat, lon        !! annotations for f2py processor
      integer*4 noutlat, noutlon
      real*4 outlat(noutlat), outlon(noutlon)
c                 ! lat/lon grid to interpolate to
cf2py intent(in) outlat, outlon
      real*4 wlat, wlon
c                 ! window of lat/lon neighbors to gaussian weight, expressed in delta lat (degrees)
c                 ! if an integer, then it is the number of neighbors on EACH side
cf2py intent(in) wlat, wlon
      real*4 slat, slon, stime
c                 ! sigma for gaussian downweighting with distance in lat, lon, & time
cf2py intent(in) slat, slon, stime
      real*4 vfactor
c                 ! factor in front of gaussian expression
      real*4 missingValue
c                 ! value to mark missing values in interp result
      integer*4 verbose
c                 ! integer to set verbosity level
cf2py intent(in) vfactor, missingValue
      real*4 vinterp(noutlat, noutlon)
c                 ! interpolated variable using gaussians, missing values not counted
      real*4 vweight(noutlat, noutlon)
c                 ! weight of values interpolated (can be zero), if so should become missing value
      integer*4 status
c                 ! negative status indicates error
cf2py intent(out) vinterp, vweight, status

      integer*4 clamp
      integer*4 imin, imax, jmin, jmax, itmp
      integer*4 iin, jin, kin
      integer*4 iMidTime
      integer*4 i, j, iwlat, iwlon

      real*4 wlat2, wlon2, lat1, lon1, dlat, dlon, fac
      real*4 varmin, varmax, val
      real*8 midTime
      logical*1 nLatNeighbors, nLonNeighbors

      write(6, *) 'Echoing inputs ...'
      write(6, *) 'ntime, nlat, nlon:', ntime, nlat, nlon
      write(6, *) 'noutlat, noutlon:', noutlat, noutlon
      write(6, *) 'wlat, wlon:', wlat, wlon
      write(6, *) 'slat, slon, stime:', slat, slon, stime
      write(6, *) 'vfactor:', vfactor
      write(6, *) 'missingValue:', missingValue

      status = 0
      if (int(wlat) .eq. wlat) then
         iwlat = int(wlat)
         nLatNeighbors = .TRUE.
         write(6, *) 'Using', iwlat,
     &      'neighbors on each side in the lat direction.'
      else
         nLatNeighbors = .FALSE.
      end if
      if (int(wlon) .eq. wlon) then
         iwlon = int(wlon)
         nLonNeighbors = .TRUE.
         write(6, *) 'Using', iwlon,
     &      'neighbors on each side in the lon direction.'
      else
         nLonNeighbors = .FALSE.
      end if

      wlat2 = wlat / 2.
      wlon2 = wlon / 2.
      lat1 = lat(1)
      lon1 = lon(1)
      dlat = lat(2) - lat(1)
      dlon = lon(2) - lon(1)
      iMidTime = int(ntime/2 + 0.5)
      midTime = time(iMidTime)

      if (verbose .gt. 3) then
          write(6, *) 'time:', time
          write(6, *) 'lat:', lat
          write(6, *) 'lon:', lon
          write(6, *) 'outlat:', outlat
          write(6, *) 'outlon:', outlon
c          write(6, *) 'mask(iMidTime):', mask(iMidTime,:,:)
          write(6, *) 'var(iMidTime):', var(iMidTime,:,:)
      end if

      do i = 1, noutlat
         if (verbose .gt. 1) write(6, *) outlat(i)
         do j = 1, noutlon
            vinterp(i,j) = 0.0
            vweight(i,j) = 0.0
            if (verbose .gt. 3) then
               write(6, *) '(i,j) = ', i, j
               write(6, *) '(outlat,outlon) = ', outlat(i), outlon(j)
            end if

            if (nLatNeighbors) then
               imin = clamp(i - iwlat, 1, nlat)
               imax = clamp(i + iwlat, 1, nlat)
            else
               imin = clamp(int((outlat(i) - wlat2 - lat1)/dlat + 0.5),
     &                      1, nlat)
               imax = clamp(int((outlat(i) + wlat2 - lat1)/dlat + 0.5),
     &                      1, nlat)
            end if
            if (imin .gt. imax) then
c              ! input latitudes descending, so swap
               itmp = imin
               imin = imax
               imax = itmp
            end if

            if (nLonNeighbors) then
               jmin = clamp(j - iwlon, 1, nlon)
               jmax = clamp(j + iwlon, 1, nlon)
            else
               jmin = clamp(int((outlon(j) - wlon2 - lon1)/dlon + 0.5),              
     &                      1, nlon)     
               jmax = clamp(int((outlon(j) + wlon2 - lon1)/dlon + 0.5),
     &                      1, nlon)
            end if

            if (verbose .gt. 2) then
               write(6, *) '(imin,imax,minlat,maxlat) = ',
     &               imin, imax, lat(imin), lat(imax)
               write(6, *) '(jmin,jmax,minlon,maxlon) = ',
     &               jmin, jmax, lon(jmin), lon(jmax)
            end if

            if (verbose .gt. 4 .and. i .eq. noutlat/2
     &             .and. j .eq. noutlon/2) then
               write(6, *) 'Echoing factors ...'
            end if

            do kin = 1, ntime
               varmin = 0.
               varmax = 0.
               do iin = imin, imax
                  do jin = jmin, jmax
                     if (mask(kin,iin,jin) .eq. 0) then
                        fac = exp( vfactor *
     &                         (((outlat(i) - lat(iin))/slat)**2
     &                        + ((outlon(j) - lon(jin))/slon)**2
     &                        + ((midTime   - time(kin))/stime)**2))

                        val = var(kin,iin,jin)
                        if (verbose .gt. 4 .and. i .eq. noutlat/2
     &                         .and. j .eq. noutlon/2) then
                           write(6, *) kin, iin, jin, time(kin),
     &                           lat(iin), lon(jin), val, fac, val*fac
                        end if

                        vinterp(i,j) = vinterp(i,j) + val * fac
                        vweight(i,j) = vweight(i,j) + fac
                        if (verbose .gt. 3) then
                           varmin = min(varmin, val)
                           varmax = max(varmax, val)
                        end if
                     end if
                  end do
               end do
               if (verbose .gt. 3) then
                  write(6, *) '(itime, varmin, varmax) = ',
     &                           kin, varmin, varmax
               end if
            end do
            if (vweight(i,j).ne. 0.0) then
               vinterp(i,j) = vinterp(i,j) / vweight(i,j)
            else
               vinterp(i,j) = missingValue
            end if
         end do
      end do
      return
      end


      integer*4 function clamp(i, n, m)
      integer*4 i, n, m
      clamp = i
      if (i .lt. n) clamp = n
      if (i .gt. m) clamp = m
      end


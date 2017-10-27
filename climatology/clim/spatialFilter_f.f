c
c spatialFilter routine -- Apply a fixed spatial filter (smoother) in lat/lon and then average over times/grids
c
c Designed to be called from python using f2py.
c
c
c Low Pass Filter = 1/9  * [1 1 1         ! normalization = 9
c                           1 1 1
c                           1 1 1]
c
c Gaussian Filter = 1/16 * [1 2 1         ! normalization = 16
c                           2 4 2
c                           1 2 1]
c

      subroutine spatialFilter_f(var, mask,
     &                         time, lat, lon,
     &                         filter, normalization,
     &                         missingValue,
     &                         verbose,
     &                         vinterp, vcount, status,
     &                         ntime, nlat, nlon)

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

      integer*4 filter(3, 3)
c                 ! 3x3 filter coefficients as integers
      integer*4 normalization
c                 ! Normalization factor for the filter, divide by sum of integers
cf2py intent(in) filter, normalization

      real*4 missingValue
c                 ! value to mark missing values in interp result
      integer*4 verbose
c                 ! integer to set verbosity level
cf2py intent(in) missingValue, verbose

      real*4 vinterp(nlat, nlon)
c                 ! interpolated variable using gaussians, missing values not counted
      integer*4 vcount(nlat, nlon)
c                 ! count of good data, might be zero after masking
      integer*4 status
c                 ! negative status indicates error
cf2py intent(out) vinterp, vcount, status

      integer*4 iin, jin, kin
      integer*4 i, j, fac, count
      real*4 val, sum

      write(6, *) 'Echoing inputs ...'
      write(6, *) 'ntime, nlat, nlon:', ntime, nlat, nlon
      write(6, *) 'filter:', filter
      write(6, *) 'normalization', normalization
      write(6, *) 'missingValue:', missingValue

      status = 0

      if (verbose .gt. 3) then
          write(6, *) 'time:', time
          write(6, *) 'lat:', lat
          write(6, *) 'lon:', lon
c          write(6, *) 'mask(3):', mask(3,:,:)
          write(6, *) 'var(3):', var(3,:,:)
      end if

      do i = 1, nlat
         if (verbose .gt. 1) write(6, *) lat(i)
         do j = 1, nlon
            vinterp(i,j) = 0.0
            vcount(i,j) = 0.0
            if (verbose .gt. 3) then
               write(6, *) '(i,j) = ', i, j
               write(6, *) '(lat,lon) = ', lat(i), lon(j)
            end if

            do kin = 1, ntime
               sum = 0.0
               count = 0
               do iin = -1, +1
                  if (i+iin .lt. 1 .or. i+iin .gt. nlat) cycle
                  do jin = -1, +1
                     if (j+jin .lt. 1 .or. j+jin .gt. nlon) cycle

                     if (mask(kin,iin,jin) .eq. 0) then
                        fac = filter(iin+2, jin+2)
                        val = var(kin,iin,jin)
                        sum = sum + fac * val
                        count = count + fac
                     end if
                  end do
               end do
               if (count .gt. 0) then
c                 ! filter for (i,j) pixel isn't empty
                  vinterp(i,j) = vinterp(i,j) + sum / normalization
                  vcount(i,j) = vcount(i,j) + 1
               end if
            end do
            if (vcount(i,j) .gt. 0) then
               vinterp(i,j) = vinterp(i,j) / vcount(i,j)
c              ! compute mean over number of non-empty times/grids
            else
               vinterp(i,j) = missingValue
            end if
         end do
      end do
      return
      end


c $Revision: 2.4 $
c     subprogram binsum

c-- calculate interpolation weights and vsum

      subroutine binsum( nf, x, y, z, f, 
     &			 xx, yy, zz, vsum,
     &			 ixmin, ixmax, iymin,
     &			 iymax, izmin, izmax, 
     &			 imax, jmax, kmax, ndatamax )

      real*4 x(ndatamax),y(ndatamax),z(ndatamax),f(ndatamax)
      real*4 xx(imax),yy(jmax),zz(kmax)
      real*4 vsum(2,imax,jmax,kmax)
      integer*4 ixmin(ndatamax),ixmax(ndatamax)
      integer*4 iymin(ndatamax),iymax(ndatamax)
      integer*4 izmin(ndatamax),izmax(ndatamax)

      common /parms/ imin,jmin,kmin,
     &               xwin2,ywin2,zwin2,
     &               xh,yh,zh,
     &               dx,dy,dz,
     &               xlo,xhi,ylo,yhi,zlo,
     &               dxd,dyd

      data xfac/-0.6931/

      do n=1,nf
        ixmin(n)=max(nint((x(n)-xwin2-xlo+0.5*dx)/dx),1)
        ixmax(n)=min(nint((x(n)+xwin2-xlo+0.5*dx)/dx),imax)
        iymin(n)=max(nint((y(n)-ywin2-ylo+0.5*dy)/dy),1)
        iymax(n)=min(nint((y(n)+ywin2-ylo+0.5*dy)/dy),jmax)
        izmin(n)=max(nint((z(n)-zwin2-zlo+0.5*dz)/dz),1)
        izmax(n)=min(nint((z(n)+zwin2-zlo+0.5*dz)/dz),kmax)
c        print *, x(n),y(n),z(n), f(n)
c	print *,' ixmin, ixmax', ixmin(n), ixmax(n)
c	print *,' iymin, iymax', iymin(n), iymax(n)
c	print *,' izmin, izmax', izmin(n), izmax(n)
      enddo



      do n=1,nf 
        do kk=izmin(n),izmax(n)
          do jj=iymin(n),iymax(n)
            do ii=ixmin(n),ixmax(n)
             
c- - this is the algorithm coded for weights

                fac=exp( xfac*(((x(n)-xx(ii))/xh)**2
     &                       + ((y(n)-yy(jj))/yh)**2
     &			     + ((z(n)-zz(kk))/zh)**2) )

c            print *, 'x, xx,  y, yy,  z, zz, fac f',
c     &        x(n), xx(ii),  y(n), yy(jj), z(n), zz(kk), fac, f(n)

                vsum(1,ii,jj,kk)=vsum(1,ii,jj,kk)+f(n)*fac
                vsum(2,ii,jj,kk)=vsum(2,ii,jj,kk)+fac
            enddo
          enddo
        enddo
      enddo
      return
      end

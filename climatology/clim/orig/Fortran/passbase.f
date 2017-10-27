      subroutine passbase( filename )

      character*25 filename
      character*25 basename
      common /basefile/ basename

      basename = filename
      end


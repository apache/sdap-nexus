
"""
plot.py -- Simple plotting utilitites
"""

import sys, os
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

def echo2(*s): sys.stderr.write(' '.join(map(str, s)) + '\n')
def echo2n(*s): sys.stderr.write('\n'.join(map(str, s)) + '\n')
def warn(*s):  echo2('plot:', *s)
def die(*s):   warn('Error,',  *s); sys.exit()


def makeMovie(plotFiles, outFile, deletePlots=True):
    """Make MPG movie from a series of still images."""
    if type(plotFiles) == dict:
        plotFiles = [plotFiles[k] for k in sorted(plotFiles.keys())]
    cmd = 'convert ' + ' '.join(plotFiles) + ' ' + outFile
    warn(cmd)
    os.system(cmd)
    warn('Wrote movie ' + outFile)
    if deletePlots:
        for f in plotFiles: os.unlink(f)
    return outFile

def createPlot():
    fig = Figure()
    canvas = FigureCanvas(fig)
    ax = fig.add_subplot(1,1,1)
    return (fig, canvas, ax)

def labelPlot(fix, canvas, ax, title,
              xlabel=None,
              ylabel=None,
              xlim=None,
              ylim=None):
#    ax.set_title(title)
    if xlabel is not None: ax.set_xlabel(xlabel)
    if ylabel is not None: ax.set_ylabel(ylabel)
    if xlim is not None: ax.set_xlim(xlim)
    if ylim is not None: ax.set_ylim(ylim)


def createTwoPlots(figure=None, canvas=None, vertical=True):
    if figure is None:
        fig = Figure()
    else:
        fig = figure
    if canvas is None: canvas = FigureCanvas(fig)
    if vertical:
        ax1 = fig.add_subplot(2,1,1)
        ax2 = fig.add_subplot(2,1,2)
    else:
        ax1 = fig.add_subplot(1,2,1)
        ax2 = fig.add_subplot(1,2,2)
    return (fig, canvas, ax1, ax2)

def labelTwoPlots(fig, canvas, ax1, ax2,
                   title = None,
                   title1 = None,
                   xlabel1 = None, ylabel1 = None,
                   xlim1 = None, ylim1 = None,
                   title2 = None,
                   xlabel2 = None, ylabel2 = None,
                   xlim2 = None, ylim2 = None,
		  ):
    if title1 is None: title1 = title
    if title1 is not None: ax1.set_title(title1)
    if xlabel1 is not None: ax1.set_xlabel(xlabel1)
    if ylabel1 is not None: ax1.set_ylabel(ylabel1)
    if xlim1 is not None: ax1.set_xlim(xlim1)
    if ylim1 is not None: ax1.set_ylim(ylim1)
    if title2 is not None: ax2.set_title(title2)
    if xlabel2 is not None: ax2.set_xlabel(xlabel2)
    if ylabel2 is not None: ax2.set_ylabel(ylabel2)
    if xlim2 is not None: ax2.set_xlim(xlim2)
    if ylim2 is not None: ax2.set_ylim(ylim2)


def createFourPlots(figure=None, canvas=None):
    if figure is None:
        fig = Figure()
    else:
        fig = figure
    if canvas is None: canvas = FigureCanvas(fig)
    ax1 = fig.add_subplot(2,2,1)
    ax2 = fig.add_subplot(2,2,2)
    ax3 = fig.add_subplot(2,2,3)
    ax4 = fig.add_subplot(2,2,4)
    return (fig, canvas, ax1, ax2, ax3, ax4)

def labelFourPlots(fig, canvas, ax1, ax2, ax3, ax4,
                   file, sat,
                   title = None,
                   title1 = None,
                   xlabel1 = None, ylabel1 = None,
                   xlim1 = None, ylim1 = None,
                   title2 = None,
                   xlabel2 = None, ylabel2 = None,
                   xlim2 = None, ylim2 = None,
                   title3 = None,
                   xlabel3 = None, ylabel3 = None,
                   xlim3 = None, ylim3 = None,
                   title4 = None,
                   xlabel4 = None, ylabel4 = None,
                   xlim4 = None, ylim4 = None,
		  ):
    if title is None: title = file + ': ' + sat
#    fig.set_title(title)
    if title1 is None: title1 = title
    if title1 is not None: ax1.set_title(title1)
    if xlabel1 is not None: ax1.set_xlabel(xlabel1)
    if ylabel1 is not None: ax1.set_ylabel(ylabel1)
    if xlim1 is not None: ax1.set_xlim(xlim1)
    if ylim1 is not None: ax1.set_ylim(ylim1)
    if title2 is not None: ax2.set_title(title2)
    if xlabel2 is not None: ax2.set_xlabel(xlabel2)
    if ylabel2 is not None: ax2.set_ylabel(ylabel2)
    if xlim2 is not None: ax2.set_xlim(xlim2)
    if ylim2 is not None: ax2.set_ylim(ylim2)
    if title3 is not None: ax3.set_title(title3)
    if xlabel3 is not None: ax3.set_xlabel(xlabel3)
    if ylabel3 is not None: ax3.set_ylabel(ylabel3)
    if xlim3 is not None: ax3.set_xlim(xlim3)
    if ylim3 is not None: ax3.set_ylim(ylim3)
    if title4 is not None: ax4.set_title(title4)
    if xlabel4 is not None: ax4.set_xlabel(xlabel4)
    if ylabel4 is not None: ax4.set_ylabel(ylabel4)
    if xlim4 is not None: ax4.set_xlim(xlim4)
    if ylim4 is not None: ax4.set_ylim(ylim4)


class NexusRendererFactory(object):
    content_types = ["CSV", "JSON", "XML", "PNG", "NETCDF", "ZIP"]
    module = __import__(__name__)

    @classmethod
    def get_renderer(cls, request):
        content_type = request.get_content_type()
        if content_type in cls.content_types:
            renderer_name = 'Nexus' + content_type + 'Renderer'
            renderer = getattr(cls.module.nexus_tornado.request.renderers, renderer_name)
            return renderer(request)






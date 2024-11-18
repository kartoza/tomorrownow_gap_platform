"""Tomorrow Now GAP."""

import json
from urllib.parse import urlparse

import requests
from django.conf import settings
from django.http import HttpResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import TemplateView, View
from django.urls import reverse

from gap.models.preferences import Preferences


class HomeView(TemplateView):
    """Home page view."""

    template_name = 'home.html'

    def get_context_data(self, **kwargs):
        """Get context data for Home view."""
        context = super().get_context_data(**kwargs)

        preferences = Preferences.load()

        context['gap_base_context'] = json.dumps({
            'api_swagger_url': reverse('api:v1:schema-swagger'),
            'api_docs_url': preferences.documentation_url
        })

        return context


@method_decorator(csrf_exempt, name="dispatch")
class SentryProxyView(View):
    """View for handling sentry."""

    sentry_key = settings.SENTRY_DSN

    def post(self, request):
        """Post sentry data."""
        host = "sentry.io"

        envelope = request.body.decode("utf-8")
        pieces = envelope.split("\n", 1)
        header = json.loads(pieces[0])

        if "dsn" in header:
            dsn = urlparse(header["dsn"])
            project_id = int(dsn.path.strip("/"))

            sentry_url = f"https://{host}/api/{project_id}/envelope/"
            headers = {
                "Content-Type": "application/x-sentry-envelope",
            }
            response = requests.post(
                sentry_url,
                headers=headers,
                data=envelope.encode("utf-8"),
                timeout=200
            )

            return HttpResponse(response.content, status=response.status_code)

        return HttpResponse(status=400)

from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.core.exceptions import PermissionDenied


class SuperuserRequiredMixin(LoginRequiredMixin, UserPassesTestMixin):
    login_url = "/auth/sign-in"
    redirect_field_name = "next"

    def test_func(self):
        user = self.request.user
        return user.is_authenticated and user.is_superuser

    def handle_no_permission(self):
        if self.request.user.is_authenticated:
            raise PermissionDenied
        return super().handle_no_permission()

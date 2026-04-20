from django.test import Client, TestCase


class SeoSurfaceTests(TestCase):
    def setUp(self):
        self.client = Client()

    def test_home_page_emits_primary_seo_metadata(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "<title>Kumquat | Object-Based Digital Cash, Wallet, and Denomination Software</title>", html=False)
        self.assertContains(response, 'rel="canonical" href="https://kumquat.info/"', html=False)
        self.assertContains(response, 'name="robots" content="index,follow,max-image-preview:large,max-snippet:-1,max-video-preview:-1"', html=False)
        self.assertContains(response, 'property="og:title" content="Kumquat | Object-Based Digital Cash, Wallet, and Denomination Software"', html=False)
        self.assertContains(response, "application/ld+json", html=False)
        self.assertContains(response, "What is Kumquat?", html=False)

    def test_robots_txt_references_sitemap_and_blocks_private_routes(self):
        response = self.client.get("/robots.txt")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response["Content-Type"].startswith("text/plain"))
        self.assertContains(response, "Disallow: /auth/", html=False)
        self.assertContains(response, "Disallow: /dashboard", html=False)
        self.assertContains(response, "Sitemap: https://kumquat.info/sitemap.xml", html=False)

    def test_sitemap_includes_home_page(self):
        response = self.client.get("/sitemap.xml")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "<loc>http://testserver/</loc>", html=False)


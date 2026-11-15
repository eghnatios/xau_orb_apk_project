import os
import threading
import webbrowser
from kivy.app import App
from kivy.clock import Clock
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label

try:
    from jnius import autoclass
    ANDROID = True
except Exception:
    ANDROID = False


def start_android_service():
    if not ANDROID:
        return
    try:
        mActivity = autoclass("org.kivy.android.PythonActivity").mActivity
        context = mActivity.getApplicationContext()
        service_name = str(context.getPackageName()) + ".ServiceOrbservice"
        service_cls = autoclass(service_name)
        service_cls.start(mActivity, "")
    except Exception as e:
        print("Service start note:", e)


class XauOrbApkApp(App):
    def build(self):
        self.title = "XAUUSDT ORB Bot"
        root = BoxLayout(orientation="vertical", padding=30, spacing=20)
        self.status_lbl = Label(
            text="XAUUSDT ORB Midpoint Bot\nStarting background engine...",
            halign="center",
            font_size="18sp",
        )
        root.add_widget(self.status_lbl)

        open_btn = Button(
            text="Open Dashboard (127.0.0.1:8080)",
            size_hint=(1, 0.25),
            font_size="16sp",
            background_color=(0.96, 0.72, 0.26, 1),
        )
        open_btn.bind(on_press=lambda *_: webbrowser.open("http://127.0.0.1:8080"))
        root.add_widget(open_btn)

        Clock.schedule_once(self._boot, 0.5)
        return root

    def _boot(self, _dt):
        if ANDROID:
            start_android_service()
            self.status_lbl.text = (
                "Background Service Active!\n"
                "Dashboard: http://127.0.0.1:8080\n"
                "(Safe to minimize — keep Battery Unrestricted)"
            )
        else:
            import xau_orb_bot
            t = threading.Thread(
                target=lambda: (
                    xau_orb_bot.start_background_threads(force=True),
                    xau_orb_bot.app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False),
                ),
                daemon=True,
            )
            t.start()
            self.status_lbl.text = "Running on http://127.0.0.1:8080"


if __name__ == "__main__":
    XauOrbApkApp().run()

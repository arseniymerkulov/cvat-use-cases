import time
import base64
from urllib.parse import quote


from cvat_integration.cvat_manager import CVATUser, CVATOrganization, CVATProject, CVATTask, CVATManager
from settings import Settings
settings = Settings.get_instance()


if __name__ == '__main__':
    username = 'user'
    email = 'user@mail.com'
    password = 'test_password_1'

    image_1 = 'IMG_2387_jpeg_jpg.rf.09b38bacfab0922a3a6b66480f01b719.jpg'
    image_2 = 'IMG_2489_jpeg_jpg.rf.ffb357957a29cdef43f3fdfb2a13c417.jpg'

    basic_auth = f'{quote(username)}:{quote(email)}:{quote(password)}'
    basic_auth = f'{base64.b64encode(basic_auth.encode()).decode()}'

    parameters = {
        'basic_auth': basic_auth,
        'categories': ['dog', 'cat'],
        'project_id': 'dog-cat-classification',
        'webhook_target_url': settings.webhook_target_url
    }
    manager = CVATManager.init_CVAT_entities(**parameters)

    task = CVATTask('annotation-task')
    task_info = manager.recreate_task(task)
    image_path = 'image/image_0.jpg'

    with open(image_path, 'rb') as file:
        task_content = base64.b64encode(file.read())

    manager.attach_images(task, [task_content], [image_path])

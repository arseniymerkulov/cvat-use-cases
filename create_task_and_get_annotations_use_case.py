import time


from cvat_package.manager import CVATUser, CVATProject, CVATTask, CVATManager


if __name__ == '__main__':
    user = CVATUser('test_user', 'test_password_777')
    project = CVATProject('test_project', [
        {'name': 'cat'},
        {'name': 'dog'},
    ])

    manager = CVATManager(user, project)
    manager.recreate_user()
    manager.recreate_project()

    manager.clear_tasks()

    task = CVATTask('test_task')
    manager.recreate_task(task)
    manager.attach_url_images(task, ['https://picsum.photos/id/1/600/600', 'https://picsum.photos/id/2/600/600'])

    while not manager.get_task_state(task):
        print('Wait for task to complete')
        time.sleep(1)

    print(manager.get_task_annotations(task))

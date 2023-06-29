"""Decorator to send an alert when a task or a flow fails"""
from functools import wraps, update_wrapper

import prefect
from prefect.blocks.notifications import AppriseNotificationBlock
from prefect.utilities.asyncutils import is_async_fn
import os
from dotenv import load_dotenv

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Content

from logger import logger


load_dotenv()


class WrappedFlow(prefect.Flow):
    """Updates the wrapper function to a Prefect flow"""

    def __init__(self, wrapper):
        if type(wrapper.__wrapped__) != prefect.Flow:
            raise RuntimeError("This class can only wrap Prefect flows.")

        self.wrapper = wrapper
        update_wrapper(self, wrapper.__wrapped__)

    def __call__(self, *args, **kwargs):
        return self.wrapper(*args, **kwargs)


def send_email_message():
    sendgrid_client = SendGridAPIClient(os.getenv("SENDGRID_APIKEY"))

    message = Mail(
        from_email=os.getenv("SENDGRID_SENDER"), to_emails=["ikoradia@umich.edu"]
    )
    message.template_id = os.getenv("SENDGRID_RESET_PASSWORD_TEMPLATE")
    message.dynamic_template_data = {"url": "testing/errored/flow/run"}

    try:
        sendgrid_client.send(message)
    except Exception as error:
        logger.exception(error)
        raise


def alert_on_failure(block_name: str):
    """Decorator to send an alert when a task or a flow fails

    This decorator must be placed before your `@flow` or `@task` decorator.

    Args:
        block_type: Type of your notification block (.i.e, 'SlackWebhook')
        block_name: Name of your notification block (.i.e, 'test')

    Examples:
        Send a notification when a flow fails
        ```python
        from prefect import flow, task
        from prefect.blocks.notifications import SlackWebhook
        from prefect_alert import alert_on_failure

        @task
            def may_fail():
                raise ValueError()

        @alert_on_failure(block_type=SlackWebhook, block_name="test")
        @flow
        def failed_flow():
            res = may_fail()
            return res

        if __name__=="__main__":
            failed_flow()
        ```

    """

    def decorator(flow):
        if is_async_fn(flow):

            @wraps(flow)
            async def wrapper(*args, **kwargs):
                """A wrapper of an async task/flow"""
                return_state = kwargs.pop("return_state", None)
                state: prefect.State = await flow(*args, return_state=True, **kwargs)

                if state.is_failed():
                    message = str(state)
                    # sendgrid api client
                    send_email_message()
                if return_state:
                    return state
                else:
                    return state.result()

            return WrappedFlow(wrapper)
        else:

            @wraps(flow)
            def wrapper(*args, **kwargs):
                """A wrapper of a sync task/flow"""
                return_state = kwargs.pop("return_state", None)
                state: prefect.State = flow(*args, return_state=True, **kwargs)
                if state.is_failed():
                    message = str(state)
                    # sendgrid api client
                    send_email_message()
                if return_state:
                    return state
                else:
                    return state.result()

            return WrappedFlow(wrapper)

    return decorator

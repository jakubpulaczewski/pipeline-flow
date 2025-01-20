try:
    try:
        try:
            raise Exception("Can divide by 0 stupid")
        except ZeroDivisionError as excp:
            raise ZeroDivisionError("test") from excp
    except Exception as e:
        raise Exception("another exception") from e
except Exception as ex:
    print("Error::: ", ex)
    print("original cause is:::: ", ex.__cause__)
    print(ex.__context__)

import sys

def profile_dummy(x):
    #    '''
    #    Dummy function to handle 'profile' decorator used by the line_profiler
    #    '''
    return x


if not 'profile' in sys.modules['builtins'].__dict__:
#    print('initializing profile function')

    # insert profile_dummy into global namespace when line_profile is not used
    sys.modules['builtins'].__dict__['profile'] = profile_dummy
else:
#    print('not initializing profile function')
    pass


from automation import CommandSequence, TaskManager
import sys
import time

inital_time = time.time()
# The list of sites that we wish to crawl
NUM_BROWSERS = 1
# sites = ['https://www.nytimes.com', 'https://www.webmd.com']#'https://www.kiplinger.com','https://www.nytimes.com','https://www.berkeley.edu','https://www.aetna.com','https://www.reddit.com']
sites_file = sys.argv[1]
with open(sites_file, 'r') as f:
    sites = f.read().splitlines()


# Loads the default manager params
# and NUM_BROWSERS copies of the default browser params
manager_params, browser_params = TaskManager.load_default_params(NUM_BROWSERS)

# Update browser configuration (use this for per-browser settings)
for i in range(NUM_BROWSERS):
    # Record HTTP Requests and Responses
    browser_params[i]['http_instrument'] = True
    # Record cookie changes
    browser_params[i]['cookie_instrument'] = True
    # Record Navigations
    browser_params[i]['navigation_instrument'] = True
    # Record JS Web API calls
    browser_params[i]['js_instrument'] = True
    # Enable flash for all three browsers
    browser_params[i]['disable_flash'] = False
    # Record the callstack of all WebRequests made
    browser_params[i]['callstack_instrument'] = True
    # Save javascript to levelDB
    browser_params[i]['save_content'] = "script"
    browser_params[i]['save_javascript'] = True

    browser_params[i]['headless'] = True  # Launch all browsers headless

# Update TaskManager configuration (use this for crawl-wide settings)
manager_params['data_directory'] = '~/' + sys.argv[2] + '/'
manager_params['log_directory'] = '~/' + sys.argv[2] + '/'

# Instantiates the measurement platform
# Commands time out by default after 60 seconds
manager = TaskManager.TaskManager(manager_params, browser_params)

task_start_time = time.time()

start_time = time.time()

# Visits the sites
for site in sites[:2]:
    start_time = time.time()
    print("now visit: %s" % site)
    # Parallelize sites over all number of browsers set above.
    # (To have all browsers go to the same sites, add `index='**'`)
    command_sequence = CommandSequence.CommandSequence(site)

    # Start by visiting the page
    # command_sequence.get(sleep=5, timeout=100)
    command_sequence.browse(num_links=4, sleep=10, timeout=300)

    # Run commands across the three browsers (simple parallelization)
    manager.execute_command_sequence(command_sequence)
    print("--- finish %s in %s seconds ---" % (site, time.time() - start_time))

for site in sites[2:]:
    start_time = time.time()
    print("now visit: %s" % site)
    # Parallelize sites over all number of browsers set above.
    # (To have all browsers go to the same sites, add `index='**'`)
    command_sequence = CommandSequence.CommandSequence(site)

    # Start by visiting the page
    command_sequence.get(sleep=10, timeout=120)
    # command_sequence.browse(num_links=4, sleep=10, timeout=120)

    # Run commands across the three browsers (simple parallelization)
    manager.execute_command_sequence(command_sequence)
    print("--- finish %s in %s seconds ---" % (site, time.time() - start_time))

print("--- %s seconds to finish the whole task ---" %
      (time.time() - task_start_time))
# Shuts down the browsers and waits for the data to finish logging
manager.close()
print("--- %s mins to finish the whole task ---" %
      ((time.time() - inital_time) / 60))

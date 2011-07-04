import os, sys
import pdb
proj_dir = os.path.abspath(os.path.dirname(__file__))
packages_dir = os.path.join(proj_dir, 'packages')
sys.path.append(packages_dir)


from sforce.enterprise import SforceEnterpriseClient
h = SforceEnterpriseClient('wsdl.jsp.xml')
h.login('ebangham@gmail.com', '303618', 'hSyF4Sgd2AEp1yt7URAb1S2yD')

